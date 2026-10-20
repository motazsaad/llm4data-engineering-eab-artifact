#!/usr/bin/env python3
"""Produce paper tables from the experiment matrices.

Wraps aggregate.py with two things the raw summary cannot give us:

1. Control exclusion. Rows for the model `__reference__` are hand-written
   artifacts that validate the instrument. They are reported separately as
   harness validation and never mixed into a model pass rate.

2. n8n transport sensitivity. On the pinned n8n image a correct workflow can
   fail for platform reasons alone: the recorder returns HTTP 200 while n8n's
   HTTP client discards the response and retries (a hand-written control
   succeeded 4/8 on n8n versus 8/8 on Airflow). Charging that to a model would
   understate it. The harness therefore re-runs transport errors up to
   N8N_EXEC_ATTEMPTS times and records how many were needed in retries.log.

   Reporting only the retried figure would overstate n8n, so every n8n cell is
   reported twice: `n8n (no retry)`, which is the first attempt only, and
   `n8n (retry<=5)`, which is the platform as configured. The gap between them
   is a measurement artifact, not a model difference, and is reported as such.

Usage:
    python3 paper_tables.py --matrix-dir out --out out/paper [--runs 3]
"""

import argparse
import csv
import glob
import json
import os
import re
import subprocess
import sys
from collections import defaultdict

SCORED = {"PASS", "FAIL", "TIMEOUT"}
CONTROL_MODEL = "__reference__"


def as_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def load_rows(matrix_dir):
    """All matrix rows, keeping duplicates (append-only history)."""
    rows = []
    for path in sorted(glob.glob(os.path.join(matrix_dir, "matrix_*.csv"))):
        with open(path, encoding="utf-8", newline="") as fh:
            for rec in csv.DictReader(fh):
                if not rec.get("model"):
                    continue
                rows.append(rec)
    return rows


def latest_per_cell(rows):
    """Keep the last row recorded for each (model, dataset, task, platform, run).

    The matrices are append-only, so re-scoring a cell under a fixed harness
    appends a new verdict. Without this, a cell that was re-measured would be
    counted at its old score as well as its new one.
    """
    latest = {}
    for rec in rows:
        key = (rec["model"], rec["dataset"], rec["task"], rec["platform"], rec.get("run", ""))
        latest[key] = rec
    return list(latest.values())


def retries_for(matrix_dir, rec):
    """Transport retries recorded for one cell run, or None if not recorded.

    The count is written by the n8n container to retries.log. Newer runs persist
    it next to the cell result (run_one.sh exports HARNESS_RESULT_DIR); older runs
    only echoed it to run.log, so fall back to parsing that. A run that predates
    either record returns None (unknown), which the caller must not treat as zero.
    """
    model_dir = rec["model"].replace(" ", "_").replace(":", "_")
    cell_dir = os.path.join(matrix_dir, "cells", rec["dataset"], rec["task"],
                            rec["platform"], model_dir, f"run{rec.get('run','')}")
    for name in ("retries.log", "run.log"):
        path = os.path.join(cell_dir, name)
        if not os.path.exists(path):
            continue
        with open(path, encoding="utf-8", errors="replace") as fh:
            hits = re.findall(r"TRANSPORT_RETRIES=(\d+)", fh.read())
        if hits:
            return max(int(h) for h in hits)
    return None


def n8n_first_attempt(matrix_dir, rec):
    """Did the FIRST attempt deliver? Used for the no-retry view.

    retries.log records how many transport retries the run needed. A run that
    needed zero retries passed on its first attempt; a run that needed retries
    failed at least once before succeeding, so it cannot count as a first-attempt
    pass. With retries disabled the harness makes a single attempt, so the
    no-retry denominator is every scored run -- which is why a cell that needed
    two retries still counts against it.

    When the count is unrecorded (None) we return None rather than False: scoring
    an unknown as a first-attempt failure is exactly the bug that once manufactured
    a platform-retry effect where none existed.
    """
    if rec["result"] != "PASS":
        return False
    n = retries_for(matrix_dir, rec)
    if n is None:
        return None
    return n == 0


def aggregate(matrix_dir, rows):
    """Pass rates per (dataset, task, platform, model), with the n8n split."""
    cells = defaultdict(lambda: {
        "replicates": 0, "passes": 0, "scored": 0, "infra": 0,
        "timeouts": 0, "rows": [],
        # n8n only: the same runs viewed as if retries did not exist
        "n8n_first_pass": 0, "n8n_scored": 0, "n8n_retries": [],
        "n8n_unknown": 0,
    })

    for rec in rows:
        model = rec["model"]
        result = (rec.get("result") or "").strip()
        key = (rec["dataset"], rec["task"], rec["platform"], model)
        c = cells[key]
        c["replicates"] += 1

        if result == "INFRASTRUCTURE":
            c["infra"] += 1
            continue
        if result not in SCORED:
            continue

        c["scored"] += 1
        if result == "PASS":
            c["passes"] += 1
        if result == "TIMEOUT":
            c["timeouts"] += 1
        rows_n = as_int(rec.get("rows"))
        if rows_n is not None:
            c["rows"].append(rows_n)

        if rec["platform"] == "n8n":
            c["n8n_scored"] += 1
            first = n8n_first_attempt(matrix_dir, rec)
            if first is True:
                c["n8n_first_pass"] += 1
            elif first is None:
                c["n8n_unknown"] += 1
            n = retries_for(matrix_dir, rec)
            if n is not None:
                c["n8n_retries"].append(n)

    return cells


def fmt_rate(passes, scored):
    if not scored:
        return "--"
    return f"{passes / scored:.2f}"


def fmt_mean(vals):
    return f"{sum(vals) / len(vals):.1f}" if vals else "--"


def paper_platform(platform, cell):
    """Expand n8n into its two reporting views.

    `n8n (retry)` is the platform as configured: transport errors are retried up
    to N8N_EXEC_ATTEMPTS times. `n8n (no retry)` is the same cell scored as if
    only the first attempt counted, which is the honest floor for a platform
    whose HTTP client fails on transport alone.
    """
    out = []
    if platform == "n8n":
        out.append(("n8n\n(retry)", cell["passes"], cell["scored"], ""))
        if cell["n8n_scored"]:
            out.append(("n8n\n(no retry)", cell["n8n_first_pass"],
                        cell["n8n_scored"], ""))
    else:
        out.append((platform, cell["passes"], cell["scored"], ""))
    return out


def build_rows(cells, controls):
    """One record per (dataset, task, model, platform-view), for LaTeX."""
    records = []
    for (ds, task, plat, model), c in sorted(cells.items()):
        if model == CONTROL_MODEL:
            continue
        for label, passes, scored, _ in paper_platform(plat, c):
            records.append({
                "dataset": ds, "task": task, "model": model, "platform": label,
                "replicates": c["replicates"], "scored": scored,
                "passes": passes, "infra": c["infra"],
                "pass_rate": fmt_rate(passes, scored),
                "coverage": fmt_rate(scored, c["replicates"]),
                "mean_rows": fmt_mean(c["rows"]),
                "timeouts": c["timeouts"],
            })
    for c in controls:
        records.append(c)
    return records


def tex_escape(s):
    return (s.replace("_", r"\_").replace("&", r"\&").replace("%", r"\%")
             .replace("#", r"\#"))


def short_model(name):
    """Compact model labels for table columns."""
    return {
        "Nemotron 3 Super 120B": "Nemo-3S",
        "Nemotron 3.5 Lightning 30B": "Nemo-3.5L",
        "Nemotron 3 Ultra 550B": "Nemo-3U",
        "Gemma 4 31B": "Gemma-4",
        "Qwen3.8 27B": "Qwen-3.8",
        "Laguna 2.1 S": "Laguna-2.1",
        "gemma4:12b": "gemma4-12B",
        "qwen3.8:27b": "qwen3.8-27B",
        "cogito:32b": "cogito-32B",
        "qwen3-coder:30b": "qwen3-coder-30B",
    }.get(name, name)


CONTROL_FIELDS = ["dataset", "task", "platform", "passes", "replicates",
                  "pass_rate", "control", "retries"]


def write_csv(records, path):
    """Model rows and control rows have different shapes, so union the keys."""
    if not records:
        return
    fields = list(records[0])
    for r in records:
        for k in r:
            if k not in fields:
                fields.append(k)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, restval="", extrasaction="ignore")
        w.writeheader()
        w.writerows(records)


def write_latex(records, path, controls):
    lines = []
    add = lines.append
    add("% Auto-generated by paper_tables.py -- do not edit by hand.")
    add(r"\begin{tabular}{@{}llrrrr@{}}")
    add(r"\toprule")
    add(r"\textbf{Dataset} & \textbf{Task} & \textbf{Model} & \textbf{Platform} "
        r"& \textbf{Pass} & \textbf{n} \\")
    add(r"\midrule")
    for r in records:
        if "control" in r:
            continue
        # A newline inside the platform cell is legal LaTeX but reads poorly in a
        # CSV; use a compact label and let the caption explain it.
        plat = r["platform"].replace("\n", " ")
        add(f"{tex_escape(r['dataset'])} & {tex_escape(r['task'])} & "
            f"{tex_escape(short_model(r['model']))} & "
            f"{tex_escape(plat)} & {r['pass_rate']} & {r['scored']} \\\\")
    add(r"\bottomrule")
    add(r"\end{tabular}")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def write_latex_long(records, path):
    """Full-coverage variant for the appendix."""
    lines = []
    add = lines.append
    add("% Auto-generated by paper_tables.py -- do not edit by hand.")
    add(r"\begin{tabular}{@{}llrrrrr@{}}")
    add(r"\toprule")
    add(r"\textbf{Dataset} & \textbf{Task} & \textbf{Model} & \textbf{Platform} "
        r"& \textbf{Pass} & \textbf{n} & \textbf{Cov.} & \textbf{Infra} \\")
    add(r"\midrule")
    for r in records:
        if "control" in r:
            continue
        add(f"{tex_escape(r['dataset'])} & {tex_escape(r['task'])} & "
            f"{tex_escape(short_model(r['model']))} & "
            f"{tex_escape(r['platform'])} & {r['pass_rate']} & {r['scored']} & "
            f"{r['coverage']} & {r['infra']} \\\\")
    add(r"\bottomrule")
    add(r"\end{tabular}")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def write_controls_latex(controls, path):
    lines = ["% Auto-generated by paper_tables.py -- do not edit by hand.",
             r"\begin{tabular}{@{}lllccl@{}}", r"\toprule",
             r"\textbf{Dataset} & \textbf{Task} & \textbf{Platform} "
             r"& \textbf{Passes} & \textbf{n} & \textbf{Retries} \\",
             r"\midrule"]
    for c in controls:
        lines.append(
            f"{tex_escape(c['dataset'])} & {tex_escape(c['task'])} & "
            f"{tex_escape(c['platform'])} & {c['passes']}/{c['replicates']} & "
            f"{c['replicates']} & {c['retries']} \\\\")
    lines += [r"\bottomrule", r"\end{tabular}"]
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def write_summary_latex(cells, path):
    """Compact pass@1 table: model x task, pooled over datasets/platforms.

    The full per-cell matrix is an appendix artifact; the body needs one readable
    table. For n8n we use the platform-as-configured (retry) view, consistent with
    the main table; the no-retry floor is reported separately.
    """
    tasks = ["T1", "T2", "T3"]
    models = sorted({m for (_, _, _, m) in cells if m != CONTROL_MODEL},
                    key=lambda m: short_model(m))
    agg = defaultdict(lambda: [0, 0])          # (model, task) -> [passes, scored]
    tot = defaultdict(lambda: [0, 0])          # model -> [passes, scored]
    for (ds, task, plat, model), c in cells.items():
        if model == CONTROL_MODEL:
            continue
        if plat == "n8n":
            p, s = c["passes"], c["scored"]
        else:
            p, s = c["passes"], c["scored"]
        agg[(model, task)][0] += p
        agg[(model, task)][1] += s
        tot[model][0] += p
        tot[model][1] += s
    lines = ["% Auto-generated by paper_tables.py -- do not edit by hand.",
             r"\begin{tabular}{@{}lrrrr@{}}", r"\toprule",
             r"\textbf{Model} & \textbf{T1 ingest} & \textbf{T2 ETL} "
             r"& \textbf{T3 join} & \textbf{Overall} \\", r"\midrule"]
    for m in models:
        cells_txt = []
        for t in tasks:
            p, s = agg[(m, t)]
            cells_txt.append(fmt_rate(p, s))
        p, s = tot[m]
        lines.append(f"{tex_escape(short_model(m))} & " +
                     " & ".join(cells_txt) + f" & {fmt_rate(p, s)} \\\\")
    # pooled row
    allp = sum(v[0] for v in agg.values())
    alls = sum(v[1] for v in agg.values())
    task_txt = []
    for t in tasks:
        p = sum(agg[(m, t)][0] for m in models)
        s = sum(agg[(m, t)][1] for m in models)
        task_txt.append(fmt_rate(p, s))
    lines.append(r"\midrule")
    lines.append(r"\textit{All models} & " + " & ".join(task_txt) +
                 f" & {fmt_rate(allp, alls)} \\\\")
    lines += [r"\bottomrule", r"\end{tabular}"]
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def write_repair_summary_latex(summary_csv, path):
    """Compact repair table: pass@1 vs pass@R pooled by slice."""
    if not os.path.exists(summary_csv):
        return 0
    with open(summary_csv, encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    def pool(pred, label):
        k = sum(int(r["k"]) for r in rows if pred(r))
        b = sum(int(r["base_pass"]) for r in rows if pred(r))
        R = sum(int(r["base_pass"]) + int(r["recovered"]) for r in rows if pred(r))
        return (label, fmt_rate(b, k), fmt_rate(R, k), str(R - b))
    slices = [
        pool(lambda r: True, r"\textit{All cells}"),
        pool(lambda r: r["task"] == "T1", "T1 ingest"),
        pool(lambda r: r["task"] == "T2", "T2 ETL"),
        pool(lambda r: r["task"] == "T3", "T3 join"),
        pool(lambda r: r["platform"] == "Airflow", "Airflow"),
        pool(lambda r: r["platform"] == "n8n", "n8n"),
    ]
    for m in sorted({r["model"] for r in rows}, key=short_model):
        slices.append(pool(lambda r, m=m: r["model"] == m, short_model(m)))
    lines = ["% Auto-generated by paper_tables.py -- do not edit by hand.",
             r"\begin{tabular}{@{}lrrr@{}}", r"\toprule",
             r"\textbf{Slice} & \textbf{pass@1} & \textbf{pass@$R$} "
             r"& \textbf{Rec.} \\", r"\midrule"]
    for label, p1, pR, rec in slices:
        lines.append(f"{tex_escape(label)} & {p1} & {pR} & {rec} \\\\")
    lines += [r"\bottomrule", r"\end{tabular}"]
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    return len(slices)


def write_repair_latex(summary_csv, path, only_dataset=None):
    """Emit a pass@1 vs pass@R table from aggregate_repair's summary CSV."""
    if not os.path.exists(summary_csv):
        return 0
    with open(summary_csv, encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    if only_dataset:
        rows = [r for r in rows if r["dataset"] == only_dataset]
    lines = ["% Auto-generated by paper_tables.py -- do not edit by hand.",
             r"\begin{tabular}{@{}llllrrrr@{}}", r"\toprule",
             r"\textbf{Dataset} & \textbf{Task} & \textbf{Platform} & \textbf{Model} "
             r"& \textbf{pass@1} & \textbf{pass@R} & \textbf{Rec.} & \textbf{k} \\",
             r"\midrule"]
    for r in rows:
        lines.append(
            f"{tex_escape(r['dataset'])} & {tex_escape(r['task'])} & "
            f"{tex_escape(r['platform'])} & {tex_escape(short_model(r['model']))} & "
            f"{r['pass@1']} & {r['pass@R']} & {r['recovered']} & {r['k']} \\\\")
    lines += [r"\bottomrule", r"\end{tabular}"]
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")
    return len(rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--matrix-dir", default="out")
    ap.add_argument("--out", default="out/paper")
    args = ap.parse_args()

    rows = latest_per_cell(load_rows(args.matrix_dir))
    cells = aggregate(args.matrix_dir, rows)

    controls = []
    for (ds, task, plat, model), c in sorted(cells.items()):
        if model != CONTROL_MODEL:
            continue
        rets = c["n8n_retries"]
        controls.append({
            "dataset": ds, "task": task, "platform": plat,
            "passes": c["passes"], "replicates": c["replicates"],
            "pass_rate": fmt_rate(c["passes"], c["replicates"]),
            "control": True,
            "retries": f"max {max(rets)}" if rets else "n/a",
        })

    records = build_rows(cells, controls)
    os.makedirs(args.out, exist_ok=True)
    write_csv(records, os.path.join(args.out, "paper_cells.csv"))
    write_latex(records, os.path.join(args.out, "paper_main_table.tex"), controls)
    write_latex_long(records, os.path.join(args.out, "paper_full_table.tex"))
    write_controls_latex(controls, os.path.join(args.out, "paper_controls_table.tex"))
    write_summary_latex(cells, os.path.join(args.out, "paper_summary_table.tex"))
    n_rep = write_repair_latex(os.path.join("out", "repair", "repair_summary.csv"),
                               os.path.join(args.out, "paper_repair_table.tex"))
    write_repair_summary_latex(os.path.join("out", "repair", "repair_summary.csv"),
                               os.path.join(args.out, "paper_repair_summary_table.tex"))
    # Dataset-split full tables: a single 72-row float is taller than a page.
    for ds in ("github", "spacex"):
        write_latex([r for r in records if r.get("dataset") == ds],
                    os.path.join(args.out, f"paper_main_table_{ds}.tex"), [])
        write_repair_latex(os.path.join("out", "repair", "repair_summary.csv"),
                           os.path.join(args.out, f"paper_repair_table_{ds}.tex"),
                           only_dataset=ds)
    print(f"cells: {len(records)}  controls: {len(controls)}  repair_rows: {n_rep}")
    for c in controls:
        print(f"  control {c['dataset']}/{c['task']}/{c['platform']}: "
              f"{c['pass_rate']} retries={c['retries']}")
    print(f"written to {args.out}/")


if __name__ == "__main__":
    main()
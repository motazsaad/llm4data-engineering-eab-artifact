#!/usr/bin/env python3
"""Aggregate per-cell matrix rows into replicate-level statistics.

One generation per cell is not a measurement: repeating an identical cell can
flip between PASS and FAIL (observed on Nemotron / SpaceX / T1 / Airflow). Every
cell is therefore run k times as independent generations, and this script
reduces those replicates to a pass rate plus a coverage figure, so that
provider outages are never silently folded into a model's failure rate.

Reads out/matrix_<dataset>_<task>.csv and writes, per (dataset, task, platform,
model):

    replicates      total runs recorded
    passes          runs whose result is PASS
    pass_rate       passes / scored_runs          (scored = PASS or FAIL)
    infra_runs      runs recorded as INFRASTRUCTURE (provider unavailable)
    coverage        scored_runs / replicates      (how much of the plan ran)
    mean_rows       mean rows delivered over scored runs
    timeouts        scored runs killed by the wall-clock budget
    unclassified    runs whose result was neither PASS/FAIL/TIMEOUT nor INFRASTRUCTURE

Usage:
    python3 aggregate.py [--matrix-dir out] [--format csv|md|both] [--out out/summary]
"""

import argparse
import csv
import glob
import json
import os
from collections import OrderedDict, defaultdict

SCORED = {"PASS", "FAIL", "TIMEOUT"}


def load_rows(matrix_dir):
    rows = []
    for path in sorted(glob.glob(os.path.join(matrix_dir, "matrix_*.csv"))):
        with open(path, encoding="utf-8", newline="") as fh:
            for rec in csv.DictReader(fh):
                if not rec.get("model"):
                    continue
                rows.append({
                    "model": rec["model"],
                    "dataset": rec["dataset"],
                    "task": rec["task"],
                    "platform": rec["platform"],
                    "run": rec.get("run", ""),
                    "result": (rec.get("result") or "").strip(),
                    "deliveries": rec.get("deliveries", ""),
                    "rows": rec.get("rows", ""),
                    "app_exit": rec.get("app_exit", ""),
                })
    return rows


def as_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def aggregate(rows):
    groups = defaultdict(list)
    for r in rows:
        # An app-level crash (the platform exited non-zero) is a model failure:
        # the artifact did not run. Older matrix rows recorded it with an empty
        # result, so classify on app_exit too.
        if r["result"] not in SCORED | {"INFRASTRUCTURE"}:
            exit_code = as_int(r["app_exit"])
            if r["result"] == "" and exit_code not in (None, 0):
                r["result"] = "FAIL"
        groups[(r["dataset"], r["task"], r["platform"], r["model"])].append(r)

    out = []
    for (dataset, task, platform, model), recs in groups.items():
        # Keep the newest record per replicate: re-running a run index overwrites
        # its matrix line, and the last attempt is the one that counts.
        by_run = OrderedDict()
        for r in recs:
            by_run[r["run"]] = r
        recs = list(by_run.values())

        scored = [r for r in recs if r["result"] in SCORED]
        infra = [r for r in recs if r["result"] == "INFRASTRUCTURE"]
        passes = [r for r in scored if r["result"] == "PASS"]
        timeouts = [r for r in scored if r["result"] == "TIMEOUT"]
        unclassified = [r for r in recs if r["result"] not in SCORED | {"INFRASTRUCTURE"}]

        row_vals = [as_int(r["rows"]) for r in scored]
        row_vals = [v for v in row_vals if v is not None]

        out.append({
            "dataset": dataset,
            "task": task,
            "platform": platform,
            "model": model,
            "replicates": len(recs),
            "scored": len(scored),
            "passes": len(passes),
            "pass_rate": round(len(passes) / len(scored), 3) if scored else None,
            "infra_runs": len(infra),
            "timeouts": len(timeouts),
            "coverage": round(len(scored) / len(recs), 3) if recs else None,
            "mean_rows": round(sum(row_vals) / len(row_vals), 2) if row_vals else None,
            "unclassified": len(unclassified),
        })
    out.sort(key=lambda r: (r["dataset"], r["task"], r["platform"], r["model"]))
    return out


FIELDS = ["dataset", "task", "platform", "model", "replicates", "scored", "passes",
          "pass_rate", "infra_runs", "timeouts", "coverage", "mean_rows", "unclassified"]


def to_csv(agg, path):
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(agg)


def to_md(agg, path):
    lines = ["| " + " | ".join(FIELDS) + " |",
             "|" + "|".join("---" for _ in FIELDS) + "|"]
    for r in agg:
        lines.append("| " + " | ".join("" if r[f] is None else str(r[f]) for f in FIELDS) + " |")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--matrix-dir", default="out")
    ap.add_argument("--out", default="out/summary")
    ap.add_argument("--format", default="both", choices=["csv", "md", "both"])
    args = ap.parse_args()

    rows = load_rows(args.matrix_dir)
    if not rows:
        raise SystemExit(f"no matrix rows found in {args.matrix_dir}")
    agg = aggregate(rows)

    os.makedirs(args.out, exist_ok=True)
    if args.format in ("csv", "both"):
        p = os.path.join(args.out, "summary.csv")
        to_csv(agg, p)
        print(p)
    if args.format in ("md", "both"):
        p = os.path.join(args.out, "summary.md")
        to_md(agg, p)
        print(p)
    with open(os.path.join(args.out, "summary.json"), "w", encoding="utf-8") as fh:
        json.dump(agg, fh, indent=2)
    print(f"[aggregate] {len(agg)} cells from {len(rows)} matrix rows")


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""Aggregate the repair experiment: recovery after feedback rounds.

Reads baseline results (out/matrix_*.csv) and repair manifests
(out/repair/manifests/<dataset>/<task>/<platform>/<safe_model>/<base_run>/result.txt)
and reports, per (dataset, task, platform, model):

  k            baseline replicate count
  base_pass    runs that passed at single shot
  recovered    baseline FAILs that passed after 1..R feedback rounds
  pass@1       base_pass / k
  pass@R       (base_pass + recovered) / k
  mean_rounds  mean feedback rounds needed among recovered runs

Writes out/repair/repair_summary.csv and .md.
"""
import argparse
import csv
import glob
import os
import re
import statistics
from collections import defaultdict


def safe_model(m):
    # Must match run_one.sh / repair_one.sh: replace only spaces and colons.
    return m.replace(" ", "_").replace(":", "_")


def load_baseline(matrix_dir):
    runs = defaultdict(list)  # key -> list of (run, passed)
    for f in sorted(glob.glob(os.path.join(matrix_dir, "matrix_*.csv"))):
        for r in csv.DictReader(open(f)):
            if r["model"] == "__reference__":
                continue
            key = (r["dataset"], r["task"], r["platform"], r["model"])
            runs[key].append((r["run"], r.get("result") == "PASS"))
    return runs


def load_repairs(repair_dir, key, run):
    d = repair_dir, key[0], key[1], key[2], safe_model(key[3]), f"run{run}"
    path = os.path.join(*d, "result.txt")
    if not os.path.exists(path):
        return None
    txt = open(path).read().strip()
    m = re.search(r"REPAIRED_IN_ROUND=(\d+)", txt)
    if m:
        return int(m.group(1))
    return 0  # not repaired


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--matrix-dir", default="out")
    ap.add_argument("--repair-dir", default="out/repair/manifests")
    ap.add_argument("--out", default="out/repair")
    args = ap.parse_args()

    base = load_baseline(args.matrix_dir)
    os.makedirs(args.out, exist_ok=True)

    rows = []
    for key in sorted(base):
        reps = base[key]
        k = len(reps)
        base_pass = sum(1 for _, ok in reps if ok)
        recovered = 0
        rounds = []
        for run, ok in reps:
            if ok:
                continue
            r = load_repairs(args.repair_dir, key, run)
            if r is None:
                continue  # repair not attempted for this run
            if r > 0:
                recovered += 1
                rounds.append(r)
        rows.append({
            "dataset": key[0], "task": key[1], "platform": key[2], "model": key[3],
            "k": k, "base_pass": base_pass, "recovered": recovered,
            "pass@1": round(base_pass / k, 3) if k else "",
            "pass@R": round((base_pass + recovered) / k, 3) if k else "",
            "mean_rounds": round(statistics.mean(rounds), 2) if rounds else "",
        })

    csv_path = os.path.join(args.out, "repair_summary.csv")
    with open(csv_path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    md_path = os.path.join(args.out, "repair_summary.md")
    with open(md_path, "w") as fh:
        fh.write("| dataset | task | platform | model | k | base_pass | recovered | pass@1 | pass@R | mean_rounds |\n")
        fh.write("|---|---|---|---|---|---|---|---|---|---|\n")
        for r in rows:
            fh.write("| {dataset} | {task} | {platform} | {model} | {k} | {base_pass} | {recovered} | {pass@1} | {pass@R} | {mean_rounds} |\n".format(**r))

    print(csv_path)
    print(md_path)
    tot_k = sum(r["k"] for r in rows)
    tot_base = sum(r["base_pass"] for r in rows)
    tot_rec = sum(r["recovered"] for r in rows)
    print(f"[repair] {len(rows)} cells, baseline {tot_base}/{tot_k} "
          f"({tot_base/tot_k:.3f}), after repair {tot_base+tot_rec}/{tot_k} "
          f"({(tot_base+tot_rec)/tot_k:.3f}), recovered {tot_rec}")


if __name__ == "__main__":
    main()

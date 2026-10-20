#!/usr/bin/env bash
# Repair sweep: re-attempt baseline FAILED runs with execution feedback.
#
# Usage:
#   bash run_matrix_repair.sh [--dataset spacex|github] [--task T1|T2|T3] \
#                             [--models m1,m2] [--rmax 2] [--per-cell]
#
# --per-cell  repair only the first failed run of each (dataset,task,platform,model)
#              cell instead of every failed run.
#
# Repair outputs go to out/repair/ (baseline in out/ is never modified). The
# recorder is shared, so runs are serialized.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

RMAX=2
DATASET=""
TASK=""
MODELS=""
PER_CELL=0
FORCE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dataset) DATASET="$2"; shift 2 ;;
    --task) TASK="$2"; shift 2 ;;
    --models) MODELS="$2"; shift 2 ;;
    --rmax) RMAX="$2"; shift 2 ;;
    --per-cell) PER_CELL=1; shift ;;
    --force) FORCE=1; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

docker compose up -d recorder >/dev/null 2>&1 || true
for _ in $(seq 1 60); do
  curl -sf http://127.0.0.1:8055/__control/ready >/dev/null 2>&1 && break
  sleep 0.5
done

LIST="$(mktemp)"
python3 - "$DATASET" "$TASK" "$MODELS" "$PER_CELL" "$FORCE" <<'PY' > "$LIST"
import csv, glob, os, sys
dataset_f, task_f, models_f, per_cell, force = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4] == "1", sys.argv[5] == "1"
models = set(m.strip() for m in models_f.split(",") if m.strip())
seen = set()
for f in sorted(glob.glob("out/matrix_*.csv")):
    for r in csv.DictReader(open(f)):
        if r["model"] == "__reference__":
            continue
        if r.get("result") != "FAIL":
            continue
        if dataset_f and r["dataset"] != dataset_f:
            continue
        if task_f and r["task"] != task_f:
            continue
        if models and r["model"] not in models:
            continue
        key = (r["dataset"], r["task"], r["platform"], r["model"])
        if per_cell and key in seen:
            continue
        seen.add(key)
        safe = r["model"].replace(" ", "_").replace(":", "_")
        manifest = f"out/repair/manifests/{r['dataset']}/{r['task']}/{r['platform']}/{safe}/run{r['run']}/result.txt"
        if os.path.exists(manifest) and not force:
            continue
        cell = f"out/cells/{r['dataset']}/{r['task']}/{r['platform']}/{safe}/run{r['run']}"
        if os.path.isdir(cell):
            print(f"{r['dataset']}\t{r['task']}\t{r['platform']}\t{r['model']}\trun{r['run']}\t{cell}")
        else:
            print(f"# missing cell dir: {cell}", file=sys.stderr)
PY

total="$(wc -l < "$LIST")"
echo "repair sweep: $total runs, rmax=$RMAX, per_cell=$PER_CELL"
i=0
while IFS=$'\t' read -r d t p m run cell; do
  [ -z "${cell:-}" ] && continue
  i=$((i+1))
  echo "########## [$i/$total] $d / $t / $p / $m / $run ##########"
  bash "$SCRIPT_DIR/repair_one.sh" --cell "$cell" --rmax "$RMAX" || true
done < "$LIST"
rm -f "$LIST"
echo "REPAIR SWEEP DONE $(date -u)"

#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
echo "=== PAPER-EXTRA START $(date -u) ==="

# A) ceiling model: qwen3-coder:30b full grid, k=3
for d in spacex github; do
  bash run_matrix.sh --models qwen3-coder --dataset "$d" --runs 3 --hermetic
done

# C) extend headline 3 models to k=5 (runs 4-5 only)
for d in spacex github; do
  bash run_matrix.sh --models "gemma4,qwen3.8,cogito" --dataset "$d" --runs 5 --run-start 4 --hermetic
done

echo "=== BASELINE AGGREGATE $(date -u) ==="
python3 aggregate.py    --matrix-dir out --out out/summary
python3 paper_tables.py --matrix-dir out --out out/paper

echo "=== REPAIR (new failed runs only) $(date -u) ==="
bash run_matrix_repair.sh --rmax 2
python3 aggregate_repair.py

echo "=== PAPER-EXTRA DONE $(date -u) ==="

#!/usr/bin/env bash
# Size-scaling ladder: one model family (qwen3) from 0.6B to 32B, full grid.
#
# Runs into an isolated output tree (MATRIX_DIR/CELLS_ROOT) so the headline
# matrices in out/ are never touched. k=5 per cell, hermetic recorder.
#
# The output dir is k-specific on purpose: a fresh tree avoids mixing replicate
# counts and drops any leftover smoke-test rows from an earlier ladder pass.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

K="${K:-5}"
OUT_DIR="${OUT_DIR:-out_scaling_k${K}}"

export MATRIX_DIR="$OUT_DIR"
export CELLS_ROOT="$OUT_DIR/cells"

MODELS="qwen3:0.6b,qwen3:1.7b,qwen3:4b,qwen3:8b,qwen3:14b,qwen3:32b"

echo "=== SCALING START $(date -u)  k=$K  out=$OUT_DIR ==="
for d in spacex github; do
  bash run_matrix.sh --models "$MODELS" --dataset "$d" --runs "$K" --hermetic
done

echo "=== SCALING TABLES $(date -u) ==="
python3 scaling_tables.py --matrix-dir "$OUT_DIR" --out out/paper

echo "=== SCALING DONE $(date -u) ==="

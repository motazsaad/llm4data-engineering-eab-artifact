#!/usr/bin/env bash
# Sweep the full experiment matrix: models x tasks x platforms x datasets x runs.
#
# --runs N runs each cell N times as independent generations and reports a pass
# rate per cell (see aggregate.py); one generation per cell is not a measurement.
#
# Usage:
#   OPENROUTER_API_KEY=sk-or-... bash run_matrix.sh [--dataset spacex|github] [--runs 3] [--hermetic] [--fail-injection N] [--use-existing]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DATASET="spacex"
RUNS=1
RUN_START=1
HERMETIC=""
FAIL_INJECTION=0
USE_EXISTING=""
ONLY_MODELS=""
ONLY_TASKS=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dataset) DATASET="$2"; shift 2 ;;
    --runs) RUNS="$2"; shift 2 ;;
    --run-start) RUN_START="$2"; shift 2 ;;
    --hermetic) HERMETIC="--hermetic"; shift ;;
    --fail-injection) FAIL_INJECTION="$2"; shift 2 ;;
    --use-existing) USE_EXISTING="--use-existing"; shift ;;
    --models) ONLY_MODELS="$2"; shift 2 ;;
    --tasks) ONLY_TASKS="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

# Kept in sync with the "models" map in prompts.json. Duplicated deliberately:
# reading it here would need a JSON parse on every invocation, and the point of
# this list is to be readable when choosing --models.
ALL_MODELS=(
  "gemma4:12b"
  "qwen3.8:27b"
  "cogito:32b"
  "qwen3-coder:30b"
  "qwen3:0.6b"
  "qwen3:1.7b"
  "qwen3:4b"
  "qwen3:8b"
  "qwen3:14b"
  "qwen3:32b"
)
PLATFORMS=("Airflow" "n8n")
ALL_TASKS=(T1 T2 T3)

# Comma-separated substring filters, so --models Nemotron,Laguna matches
# "Nemotron 3 Super 120B" and "Laguna 2.1 S" without quoting every full name.
MODELS=()
TASKS=()

# Matches an item against a comma-separated filter, as substrings, so
# --models Nemotron,Laguna selects "Nemotron 3 Super 120B" and "Laguna 2.1 S".
# An earlier exact/whole-string version silently matched nothing and ran a sweep
# of zero cells, so this asserts that a filter resolved to something.
matches() {
  local needle="$1" item="$2"
  [[ -z "$needle" ]] && return 0
  local token
  local IFS=","
  for token in $needle; do
    [[ -n "$token" && "$item" == *"$token"* ]] && return 0
  done
  return 1
}

for model in "${ALL_MODELS[@]}"; do
  matches "$ONLY_MODELS" "$model" || continue
  MODELS+=("$model")
done
for task in "${ALL_TASKS[@]}"; do
  matches "$ONLY_TASKS" "$task" || continue
  TASKS+=("$task")
done

echo "sweep: models=[${MODELS[*]}] tasks=[${TASKS[*]}] platforms=[${PLATFORMS[*]}] dataset=$DATASET runs=${RUN_START}..${RUNS}"

for model in "${MODELS[@]}"; do
  for task in "${TASKS[@]}"; do
    for platform in "${PLATFORMS[@]}"; do
      for run in $(seq "$RUN_START" "$RUNS"); do
        echo ""
        echo "################## $model / $task / $platform / $DATASET / run $run ##################"
        # Provider keys are inherited from the environment; run_one.sh resolves
        # the right one per model, so NVIDIA models work in the same sweep.
        if ! bash run_one.sh \
            --model "$model" --task "$task" --platform "$platform" \
            --dataset "$DATASET" --run "$run" ${HERMETIC} ${USE_EXISTING} \
            $( [[ "$FAIL_INJECTION" -gt 0 ]] && echo --fail-injection "$FAIL_INJECTION" ); then
          echo "[cell FAIL] $model / $task / $platform / run $run"
        fi
      done
    done
  done
done

echo ""
echo "Cells done. Combined matrices:"
ls -la out/matrix_*.csv 2>/dev/null || true

echo ""
echo "Replicate summary (pass rate + coverage):"
if [[ -f out/matrix_spacex_T1.csv || -f out/matrix_github_T1.csv ]]; then
  python3 aggregate.py --matrix-dir out --out out/summary && cat out/summary/summary.md
else
  echo "no matrix rows to aggregate"
fi
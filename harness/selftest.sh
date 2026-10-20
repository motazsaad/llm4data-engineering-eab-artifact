#!/usr/bin/env bash
# Validate the harness itself against hand-written control artifacts.
#
# If a reference artifact does not score PASS, the defect is in the measurement
# instrument, not in any model. This is the check to run before attributing a
# failure to a model, and after any change to the recorder, runners, or scorer.
#
# Usage: bash selftest.sh [--task T3] [--dataset github]
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

TASK="T3"
DATASET="github"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --task) TASK="$2"; shift 2 ;;
    --dataset) DATASET="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

MODEL="__reference__"
FAILED=0

docker compose up -d recorder >/dev/null 2>&1 || { echo "recorder failed to start" >&2; exit 2; }
for _ in $(seq 1 60); do
  curl -sf http://127.0.0.1:8055/__control/ready >/dev/null 2>&1 && break
  sleep 0.5
done

# Stage the reference artifact under the exact filename generate_artifact.py
# would have written, then run with --use-existing so no OpenRouter call is made.
run_ref() {
  local platform="$1" src="$2" run="$3"
  local cell="out/cells/$DATASET/$TASK/$platform/$MODEL/run$run"
  local ext="py"
  [[ "$platform" == "n8n" ]] && ext="json"
  # slug("__reference__") == "reference"; platform is used verbatim.
  local want="$DATASET-reference-$TASK-$platform.$ext"

  if [[ ! -f "$src" ]]; then
    echo "[SELFTEST FAIL] missing control artifact: $src" >&2
    FAILED=1
    return 1
  fi

  rm -rf "$cell"; mkdir -p "$cell"
  cp "$src" "$cell/$want" || { echo "[SELFTEST FAIL] cannot stage $src" >&2; FAILED=1; return 1; }

  echo ""
  echo "########## self-test: $platform / $TASK / $DATASET ##########"
  OPENROUTER_API_KEY="unused" bash run_one.sh \
    --model "$MODEL" --task "$TASK" --platform "$platform" \
    --dataset "$DATASET" --run "$run" --use-existing
  local rc=$?
  if [[ $rc -ne 0 ]]; then
    echo "[SELFTEST FAIL] $platform exited $rc"
    FAILED=1
  fi
}

run_ref Airflow "selftest/reference_${TASK}_${DATASET}_airflow.py" 1
run_ref n8n     "selftest/reference_${TASK}_${DATASET}_n8n.json" 1

echo ""
if [[ "$FAILED" -eq 0 ]]; then
  echo "SELFTEST OK — harness scored both reference artifacts as expected"
else
  echo "SELFTEST FAILED — investigate the harness before blaming a model"
fi
exit "$FAILED"
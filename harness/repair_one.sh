#!/usr/bin/env bash
# Repair one baseline cell with execution feedback, up to R rounds.
#
# Usage:
#   bash repair_one.sh --cell out/cells/<d>/<t>/<p>/<model>/run1 [--rmax 3]
#
# For each round it rebuilds a repair prompt (previous artifact + concise
# failure feedback + instructions) and re-runs the cell through run_one.sh,
# writing all repair artifacts to out/repair/ so the baseline is never touched.
set -euo pipefail

R_MAX=3
CELL_DIR=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --cell) CELL_DIR="$2"; shift 2 ;;
    --rmax) R_MAX="$2"; shift 2 ;;
    *) echo "usage: repair_one.sh --cell <baseline_cell_dir> [--rmax N]" >&2; exit 2 ;;
  esac
done
[ -z "$CELL_DIR" ] && { echo "usage: repair_one.sh --cell <dir> [--rmax N]" >&2; exit 2; }
[ -d "$CELL_DIR" ] || { echo "baseline cell not found: $CELL_DIR" >&2; exit 2; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

RAW="$(find "$CELL_DIR" -maxdepth 1 -name "*.raw.json" | head -1)"
[ -f "$RAW" ] || { echo "no raw.json in $CELL_DIR" >&2; exit 2; }
params=($(python3 -c "
import json
d=json.load(open('$RAW'))
print(d['dataset']); print(d['task']); print(d['platform']); print(d.get('model_display') or d.get('model') or '')
"))
DATASET="${params[0]}"; TASK="${params[1]}"; PLATFORM="${params[2]}"; MODEL="${params[3]}"
SAFE_MODEL="${MODEL// /_}"; SAFE_MODEL="${SAFE_MODEL//:/_}"
BASE_RUN="$(basename "$CELL_DIR")"

OUT_BASE="$SCRIPT_DIR/out/repair"
REPAIR_CELLS="$OUT_BASE/cells"
MANIFEST_DIR="$OUT_BASE/manifests/${DATASET}/${TASK}/${PLATFORM}/${SAFE_MODEL}/$BASE_RUN"
mkdir -p "$MANIFEST_DIR"

echo "{\"dataset\":\"$DATASET\",\"task\":\"$TASK\",\"platform\":\"$PLATFORM\",\"model\":\"$MODEL\",\"base_run\":\"$BASE_RUN\"}" \
  > "$MANIFEST_DIR/cell.json"

PREV_CELL="$CELL_DIR"
R=1
while [ "$R" -le "$R_MAX" ]; do
  echo "=== repair round $R / $R_MAX ($DATASET/$TASK/$PLATFORM/$SAFE_MODEL/$BASE_RUN) ==="
  FB="$MANIFEST_DIR/feedback_r${R}.json"
  python3 "$SCRIPT_DIR/repair_feedback.py" "$PREV_CELL" > "$FB"

  PROMPT="$MANIFEST_DIR/repair_r${R}.prompt.txt"
  {
    echo "REPAIR ATTEMPT $R: Your previous attempt FAILED the automated checks. Fix the bug(s)."
    echo
    echo "=== YOUR PREVIOUS ARTIFACT ==="
    for f in "$PREV_CELL"/*.py "$PREV_CELL"/*.json; do
      bn="$(basename "$f")"
      case "$bn" in *.prompt.txt|*.raw.json|score.json|trace.jsonl|sink.jsonl) continue;; esac
      [ -f "$f" ] && { head -c 6000 "$f"; break; }
    done
    echo
    echo "=== FAILURE FEEDBACK ==="
    cat "$FB"
    echo
    echo "=== INSTRUCTIONS ==="
    echo "Return ONLY the complete, final, corrected artifact for $DATASET/$TASK/$PLATFORM."
    echo "No explanations, no markdown fences. It must pass every failing check above."
  } > "$PROMPT"

  set +e
  CELLS_ROOT="$REPAIR_CELLS" MATRIX_DIR="$OUT_BASE" \
    bash "$SCRIPT_DIR/run_one.sh" \
      --model "$MODEL" --dataset "$DATASET" --task "$TASK" --platform "$PLATFORM" \
      --run "$R" --hermetic --repair-prompt "$PROMPT" 2>&1 | tee "$MANIFEST_DIR/test_r${R}.log"
  set -e

  CUR_CELL="$REPAIR_CELLS/$DATASET/$TASK/$PLATFORM/$SAFE_MODEL/run$R"
  if [ -f "$CUR_CELL/score.json" ]; then
    cp "$CUR_CELL/score.json" "$MANIFEST_DIR/score_r${R}.json"
  fi
  if python3 -c "
import json,sys
try:
  s=json.load(open('$MANIFEST_DIR/score_r${R}.json'))
  sys.exit(0 if s.get('passed') else 1)
except Exception:
  sys.exit(1)
"; then
    echo "REPAIRED_IN_ROUND=$R" | tee -a "$MANIFEST_DIR/result.txt"
    PREV_CELL="$CUR_CELL"; R=$((R+1)); break
  fi
  PREV_CELL="$CUR_CELL"
  R=$((R+1))
done

[ -f "$MANIFEST_DIR/result.txt" ] || echo "NOT_REPAIRED_AFTER=$R_MAX" > "$MANIFEST_DIR/result.txt"
cat "$MANIFEST_DIR/result.txt"

#!/usr/bin/env bash
# Orchestrate one cell: render prompt -> generate artifact -> run platform -> assert.
# One cell = (model, task, platform, dataset) for a single run.
#
# Default is hermetic: generated code is pointed at the local recorder, which
# replays dataset fixtures as the source and captures deliveries as the sink.
# (Live mode hits the real public APIs and cannot be auto-asserted.)
#
# Usage:
#   OPENROUTER_API_KEY=sk-or-... bash run_one.sh \
#       --model "Nemotron 3 Super 120B" --task T1 --platform Airflow \
#       --dataset spacex [--run 1] [--live] [--fail-injection N] \
#       [--require-keys a,b,c] [--use-existing]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

MODEL=""
TASK=""
PLATFORM=""
DATASET="spacex"
RUN=1
HERMETIC=1
FAIL_INJECTION=0
REQUIRE_KEYS=""
USE_EXISTING=0
REPAIR_PROMPT=""
TIMEOUT_SEC=600

while [[ $# -gt 0 ]]; do
  case "$1" in
    --model) MODEL="$2"; shift 2 ;;
    --task) TASK="$2"; shift 2 ;;
    --platform) PLATFORM="$2"; shift 2 ;;
    --dataset) DATASET="$2"; shift 2 ;;
    --run) RUN="$2"; shift 2 ;;
    --hermetic) HERMETIC=1; shift ;;
    --live) HERMETIC=0; shift ;;
    --fail-injection) FAIL_INJECTION="$2"; shift 2 ;;
    --require-keys) REQUIRE_KEYS="$2"; shift 2 ;;
    --use-existing) USE_EXISTING=1; shift ;;
    --repair-prompt) REPAIR_PROMPT="$2"; shift 2 ;;
    --timeout) TIMEOUT_SEC="$2"; shift 2 ;;
    -h|--help)
      echo "usage: run_one.sh --model M --task T1|T2|T3 --platform Airflow|n8n [--dataset spacex|github] [--run N] [--hermetic|--live] [--fail-injection N] [--require-keys k1,k2] [--use-existing] [--timeout SEC]"
      exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "$MODEL" || -z "$TASK" || -z "$PLATFORM" ]]; then
  echo "required: --model --task --platform" >&2
  exit 2
fi

STAMP="$(date -u +%Y%m%d-%H%M%S)"
# Model display names may contain characters illegal in a docker -v mount or a
# filesystem path: Ollama tags carry a colon (gemma4:12b) and would break the
# `-v host:container:ro` syntax. Sanitize spaces and colons; OpenRouter display
# names contain neither, so their cell paths are unchanged.
SAFE_MODEL="${MODEL// /_}"
SAFE_MODEL="${SAFE_MODEL//:/_}"
# Output roots are overridable so the repair experiment can persist to its own
# tree (out/repair/...) without clobbering the baseline cells.
CELLS_ROOT="${CELLS_ROOT:-out/cells}"
MATRIX_DIR="${MATRIX_DIR:-out}"
RESULT_DIR="${CELLS_ROOT}/${DATASET}/${TASK}/${PLATFORM}/${SAFE_MODEL}/run${RUN}"
mkdir -p "$RESULT_DIR"

# URLs: inside-platform containers reach recorder by compose service name;
# the orchestrator reaches it through the published host port.
BASE_URL_DOCKER="${BASE_URL_DOCKER:-http://recorder:8055}"
BASE_URL_HOST="${BASE_URL_HOST:-http://127.0.0.1:8055}"
FIXTURE_MAP="$(pwd)/fixture_map.json"
PYTHON=python3

# 0) Ensure recorder is running (hermetic)
if [[ "$HERMETIC" -eq 1 ]]; then
  docker compose up -d recorder >/dev/null 2>&1 || { echo "docker compose recorder failed" >&2; exit 2; }
  for _ in $(seq 1 60); do
    if curl -sf "$BASE_URL_HOST/__control/ready" >/dev/null 2>&1; then break; fi
    sleep 0.5
  done
  curl -sf -X POST "$BASE_URL_HOST/__control/reset" >/dev/null 2>&1 || true
fi

# 1) Render + generate artifact
MATRIX="${MATRIX_DIR}/matrix_${DATASET}_${TASK}.csv"
mkdir -p "$MATRIX_DIR"
HEADER="model,dataset,task,platform,run,hermetic,result,deliveries,rows,app_exit"
[[ -f "$MATRIX" ]] || echo "$HEADER" > "$MATRIX"
echo "== generate ($MODEL / $TASK / $PLATFORM / $DATASET) =="
GEN_ARGS=(--task "$TASK" --platform "$PLATFORM" --dataset "$DATASET" --model "$MODEL"
          --fixture-map "$FIXTURE_MAP" --out "$RESULT_DIR")
if [[ "$HERMETIC" -eq 1 ]]; then
  GEN_ARGS+=(--base-url "$BASE_URL_DOCKER")
fi
if [[ "$USE_EXISTING" -eq 1 ]]; then
  GEN_ARGS+=(--use-existing)
fi
if [[ -n "$REPAIR_PROMPT" ]]; then
  GEN_ARGS+=(--repair-prompt "$REPAIR_PROMPT")
fi
OUT="$($PYTHON generate_artifact.py "${GEN_ARGS[@]}")" && GEN_EXIT=0 || GEN_EXIT=$?
if [[ "$GEN_EXIT" -eq 3 ]]; then
  # Provider free-tier outage: the cell was never attempted. Record it as
  # `infrastructure` so coverage is reportable and it is not scored as a
  # model failure.
  RESULT=INFRASTRUCTURE
  echo "== provider unavailable; cell not attempted =="
  echo '{"passed": false, "outcome": "infrastructure", "reason": "provider_rate_limited"}' \
    > "$RESULT_DIR/score.json"
  echo "$MODEL,$DATASET,$TASK,$PLATFORM,$RUN,$HERMETIC,$RESULT,,,,3" >> "$MATRIX"
  echo "== done: INFRASTRUCTURE ($MODEL $TASK $PLATFORM run $RUN) =="
  echo "result dir: $RESULT_DIR"
  exit 0
fi
if [[ "$GEN_EXIT" -eq 4 ]]; then
  # The provider answered 200 but the model emitted no artifact (typically a
  # reasoning-only reply that consumed the whole token budget). That is a model
  # outcome, not an outage: score it as a failure so the cell stays in coverage
  # instead of silently disappearing from the matrix.
  RESULT=FAIL
  echo "== model returned no artifact; cell scored as failure =="
  echo '{"passed": false, "outcome": "no_content", "reason": "no_artifact"}' \
    > "$RESULT_DIR/score.json"
  echo "$MODEL,$DATASET,$TASK,$PLATFORM,$RUN,$HERMETIC,$RESULT,,,,4" >> "$MATRIX"
  echo "== done: FAIL ($MODEL $TASK $PLATFORM run $RUN) =="
  echo "result dir: $RESULT_DIR"
  exit 1
fi
if [[ "$GEN_EXIT" -ne 0 ]]; then
  echo "artifact generation failed (exit $GEN_EXIT)" >&2
  echo "== done: ERROR ($MODEL $TASK $PLATFORM run $RUN) =="
  exit "$GEN_EXIT"
fi
ARTIFACT="$(echo "$OUT" | tail -1)"
echo "artifact: $ARTIFACT"

# 2) Arm injected failures BEFORE running (retry behavior test)
if [[ "$HERMETIC" -eq 1 && "$FAIL_INJECTION" -gt 0 ]]; then
  echo "== arming $FAIL_INJECTION injected failure(s) =="
  curl -sf -X POST "$BASE_URL_HOST/__control/fail?times=$FAIL_INJECTION" >/dev/null || true
fi

# 3) Run platform
RUN_LOG="$RESULT_DIR/run.log"
APP_EXIT=0
# The n8n runner writes its transport-retry count to a workspace log; export the
# cell dir so it can copy the value in for downstream aggregation. Without this,
# paper_tables.retries_for() found no per-cell retries.log and scored every n8n
# pass as "not first attempt", manufacturing a platform effect that does not exist.
export HARNESS_RESULT_DIR="$RESULT_DIR"
echo "== run on $PLATFORM =="
if [[ "$PLATFORM" == "Airflow" ]]; then
  DAG_ID="$($PYTHON - "$ARTIFACT" <<'PY'
import re, sys
src = open(sys.argv[1], encoding="utf-8").read()


def const(name):
    """Resolve a module-level string constant (handles `X: str = "..."`)."""
    m = re.search(rf"\b{re.escape(name)}\b\s*(?::[^=\n]+)?=\s*[\"']([^\"']+)[\"']", src)
    return m.group(1) if m else None


# `dag_id = "literal"` is the common case, but models also write
# `DAG_ID = "..."; with DAG(dag_id=DAG_ID)`. Falling back to a guessed id in
# that case makes `airflow dags test` look for a DAG that does not exist and
# reports "failed to parse" -- a harness lookup miss, not a model failure.
m = re.search(r"dag_id\s*=\s*[\"']([^\"']+)[\"']", src)
if not m:
    m2 = re.search(r"dag_id\s*=\s*([A-Za-z_]\w*)", src)
    if m2 and const(m2.group(1)):
        print(const(m2.group(1)))
        raise SystemExit
m = (m
     or re.search(r"with\s+DAG\(\s*[\"']([^\"']+)[\"']", src)
     or re.search(r"DAG\s*\(\s*[\"']([^\"']+)[\"']", src))
print(m.group(1) if m else "")
PY
)"
  [[ -n "$DAG_ID" ]] || DAG_ID="generated_dag"
  timeout -k 15 "$TIMEOUT_SEC" ./run_airflow.sh "$ARTIFACT" "$DAG_ID" "$BASE_URL_DOCKER" > "$RUN_LOG" 2>&1 || APP_EXIT=$?
elif [[ "$PLATFORM" == "n8n" ]]; then
  timeout -k 15 "$TIMEOUT_SEC" ./run_n8n.sh "$ARTIFACT" "$BASE_URL_DOCKER" > "$RUN_LOG" 2>&1 || APP_EXIT=$?
else
  echo "unknown platform: $PLATFORM" >&2; exit 2
fi

# 4) Read recorder logs (hermetic)
if [[ "$HERMETIC" -eq 1 ]]; then
  sleep 2
  docker compose cp recorder:/out/cells/sink.jsonl "$RESULT_DIR/sink.jsonl" >/dev/null 2>&1 || true
  docker compose cp recorder:/out/cells/trace.jsonl "$RESULT_DIR/trace.jsonl" >/dev/null 2>&1 || true
else
  : > "$RESULT_DIR/sink.jsonl"
  : > "$RESULT_DIR/trace.jsonl"
fi

# 5) Assert
echo "== assert =="
ASSERT_ARGS=(--task "$TASK" --log "$RESULT_DIR/sink.jsonl" --trace "$RESULT_DIR/trace.jsonl"
             --score "$RESULT_DIR/score.json")
if [[ -z "$REQUIRE_KEYS" ]]; then
  # Schema expectations are dataset-specific: a SpaceX launch row has no
  # `full_name`, and a GitHub repo row has no `flight_number`. Resolve the
  # required key paths from prompts.json unless overridden by --require-keys.
  # Read required key paths and the expected row count together. expect_rows is
# the join cardinality implied by the fixtures; without it a cross product and
# a correct inner join score identically.
read -r REQUIRE_KEYS EXPECT_ROWS <<<"$($PYTHON - "$(pwd)/prompts.json" "$DATASET" "$TASK" <<'PYEOF'
import json, sys
cfg = json.load(open(sys.argv[1], encoding="utf-8"))
ds = cfg.get("datasets", {}).get(sys.argv[2], {})
keys = ",".join(ds.get("require_keys", {}).get(sys.argv[3], []))
print(keys, ds.get("expect_rows", {}).get(sys.argv[3], 0))
PYEOF
)"
fi
case "$TASK" in
  T1) ASSERT_ARGS+=(--min-rows 1 --min-columns 1 --require-keys "$REQUIRE_KEYS");;
  T2) ASSERT_ARGS+=(--min-rows 1 --min-columns 2 --require-keys "$REQUIRE_KEYS");;
  T3) ASSERT_ARGS+=(--min-rows 1 --min-columns 2 --require-keys "$REQUIRE_KEYS");;
esac
# Exact row count where the fixtures make it knowable. Enforces join cardinality,
# not merely that some rows arrived.
if [[ "${EXPECT_ROWS:-0}" -gt 0 ]]; then
  ASSERT_ARGS+=(--expect-rows "$EXPECT_ROWS")
fi
if [[ "$FAIL_INJECTION" -gt 0 ]]; then
  ASSERT_ARGS+=(--expect-fail-injection "$FAIL_INJECTION")
fi
RESULT=FAIL
if [[ "$APP_EXIT" -eq 124 ]]; then
  # A model-chosen retry policy (e.g. retries=3, retry_delay=5min) makes
  # `airflow dags test` poll indefinitely, so the harness must impose a wall
  # clock budget rather than hang. The artifact never delivered, so this is a
  # scored failure, reported distinctly because "hung" and "crashed" are
  # different symptoms.
  RESULT=TIMEOUT
  echo "platform run exceeded ${TIMEOUT_SEC}s budget; recorded as TIMEOUT"
  echo "{\"passed\": false, \"outcome\": \"timeout\", \"timeout_s\": $TIMEOUT_SEC}" > "$RESULT_DIR/score.json"
elif [[ "$APP_EXIT" -ne 0 ]]; then
  echo "platform run exited non-zero ($APP_EXIT); app-level failure"
  echo "{\"passed\": false, \"app_exit\": $APP_EXIT}" > "$RESULT_DIR/score.json"
elif [[ "$HERMETIC" -eq 0 ]]; then
  echo "live mode: skipping auto-assertion (no recorder sink)"
  echo "{\"passed\": true, \"app_exit\": 0, \"note\": \"live mode, unverified delivery\"}" > "$RESULT_DIR/score.json"
  RESULT=PASS
elif $PYTHON assert_results.py "${ASSERT_ARGS[@]}"; then
  RESULT=PASS
fi

# 6) Record row in matrix CSV
HEADER="model,dataset,task,platform,run,hermetic,result,deliveries,rows,app_exit"
[[ -f "$MATRIX" ]] || echo "$HEADER" > "$MATRIX"
DELIVERIES=""; ROWS=""
if [[ -f "$RESULT_DIR/score.json" ]]; then
  SCORE="$($PYTHON - "$RESULT_DIR/score.json" <<'PY'
import json,sys
s=json.load(open(sys.argv[1]))
# Pipe-delimited: an app-level crash has no deliveries/rows keys, and plain
# whitespace splitting would collapse the leading empty fields.
print("|".join([str(s.get("deliveries", "")), str(s.get("rows", "")),
                "TIMEOUT" if s.get("outcome") == "timeout"
                else ("PASS" if s.get("passed") else "FAIL")]))
PY
)"
  IFS='|' read -r DELIVERIES ROWS SCORED_RESULT <<< "$SCORE"
  # Preserve TIMEOUT: it is a scored failure, but "hung" and "crashed" are
  # different symptoms and the matrix must keep them apart.
  if [[ "$RESULT" != "TIMEOUT" ]]; then
    RESULT="$SCORED_RESULT"
  fi
fi
echo "$MODEL,$DATASET,$TASK,$PLATFORM,$RUN,$HERMETIC,$RESULT,$DELIVERIES,$ROWS,$APP_EXIT" >> "$MATRIX"

echo "== done: $RESULT ($MODEL $TASK $PLATFORM run $RUN) =="
echo "result dir: $RESULT_DIR"
[[ "$RESULT" == PASS ]]
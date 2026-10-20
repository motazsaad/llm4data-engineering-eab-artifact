#!/usr/bin/env bash
# Run one Airflow artifact against the recorder and capture execution.
# Args: ARTIFACT_PATH DAG_ID [RECORDER_BASE]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [[ $# -lt 2 ]]; then
  echo "usage: run_airflow.sh ARTIFACT_PATH DAG_ID [RECORDER_BASE]" >&2
  exit 2
fi
ARTIFACT="$(readlink -f "$1")"
DAG_ID="$2"
RECORDER_BASE="${3:-http://recorder:8055}"
NET="$(docker network ls --format '{{.Name}}' | grep -E 'harness' | head -1)"
if [[ -z "$NET" ]]; then
  echo "no harness network found; run: docker compose up -d recorder" >&2
  exit 2
fi

FNAME="$(basename "$ARTIFACT")"
docker build -q -t harness-airflow:local ./runners/airflow >/dev/null

echo "airflow running $DAG_ID from $FNAME (recorder=$RECORDER_BASE)"
docker run --rm \
  --network "$NET" \
  -e RECORDER_BASE="$RECORDER_BASE" \
  -v "$(dirname "$ARTIFACT")":/opt/airflow/dags/harness:ro \
  -v "$SCRIPT_DIR/out":/out \
  harness-airflow:local \
  "$DAG_ID" "/opt/airflow/dags/harness/$FNAME"
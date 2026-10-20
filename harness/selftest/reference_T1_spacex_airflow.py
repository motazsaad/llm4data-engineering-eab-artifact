"""Reference T1 (ingestion) DAG for SpaceX — harness self-test artifact.

Hand-written control: fetches the `/launches/latest` fixture and delivers it to
the sink unchanged. T1 is the simplest task, so a failure here means the harness
is at fault rather than any model.

Note the source is `latest.json`, which holds ONE launch object, not the 3-row
`launches.json` used by T2/T3. The expect_rows for SpaceX T1 is therefore 1. An
earlier version of this control fetched launches.json and delivered 3 rows,
which contradicted the T1 prompt and made the control unrepresentative of what
models are asked to build.

Paired with reference_T1_spacex_n8n.json, which deliberately omits a trigger
node. That exercises the path where the harness must PREPEND a Manual Trigger
instead of replacing the model's entry node; an earlier harness deleted the entry
node there and scored correct models as failures.
"""

from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

BASE = "http://recorder:8055"
SRC = BASE + "/fixtures/spacex/latest.json"
SINK = BASE + "/sink"


def fetch(url):
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def ingest_and_deliver(**context):
    launch = fetch(SRC)

    delivered = {
        "flight_number": launch.get("flight_number"),
        "mission_name": launch.get("mission_name"),
        "launch_year": launch.get("launch_year"),
        "launch_date_utc": launch.get("launch_date_utc"),
        "launch_success": launch.get("launch_success"),
        "rocket": launch.get("rocket"),
    }

    resp = requests.post(SINK, json={"data": delivered}, timeout=30)
    resp.raise_for_status()
    return 1


with DAG(
    dag_id="reference_t1_ingest_spacex",
    default_args={"owner": "harness"},
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    ingest_and_deliver_task = PythonOperator(
        task_id="ingest_and_deliver",
        python_callable=ingest_and_deliver,
    )
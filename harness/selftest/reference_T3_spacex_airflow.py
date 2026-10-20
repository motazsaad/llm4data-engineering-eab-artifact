"""Reference T3 (relational combine) DAG for SpaceX — harness self-test artifact.

Hand-written control: joins launches to rockets on rocket_id, using only the
platform's own APIs. If this does not score PASS, a 0/12 model pass rate on
SpaceX T3 would mean the harness is at fault rather than the models.

Note the fixture nests rocket fields under `launches[].rocket`, so a correct join
must read `rocket.rocket_id` rather than expecting a flat `rocket` column.
"""

from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

BASE = "http://recorder:8055"
SRC_A = BASE + "/fixtures/spacex/launches.json"
SRC_B = BASE + "/fixtures/spacex/rockets.json"
SINK = BASE + "/sink"


def fetch(url):
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def join_and_deliver(**context):
    launches = fetch(SRC_A)
    rockets = fetch(SRC_B)

    by_id = {r["rocket_id"]: r for r in rockets}

    joined = []
    for launch in launches:
        ref = launch.get("rocket") or {}
        rocket = by_id.get(ref.get("rocket_id"))
        if rocket is None:
            continue
        joined.append({
            "flight_number": launch.get("flight_number"),
            "mission_name": launch.get("mission_name"),
            "rocket_id": rocket.get("rocket_id"),
            "rocket_name": rocket.get("rocket_name"),
            "rocket_type": rocket.get("rocket_type"),
            "launch_success": launch.get("launch_success"),
        })

    resp = requests.post(SINK, json={"data": joined}, timeout=30)
    resp.raise_for_status()
    return len(joined)


with DAG(
    dag_id="reference_t3_join_spacex",
    default_args={"owner": "harness"},
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    join_and_deliver_task = PythonOperator(
        task_id="join_and_deliver",
        python_callable=join_and_deliver,
    )
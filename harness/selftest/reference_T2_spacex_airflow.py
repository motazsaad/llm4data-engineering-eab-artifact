"""Reference T2 (ETL) DAG for SpaceX — harness self-test artifact.

Hand-written control: fetches the launches fixture, selects fields, computes a
derived value, prints debug output, reports total execution time, and delivers
the transformed rows.

T2 is scored with `--min-columns 2` and requires `flight_number`, so a control
must both transform the data and preserve that key. Without a control here, a
0/N T2 pass rate would be uninterpretable: it could mean the models are wrong or
that the transformation requirements are unsatisfiable.
"""

import time
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

BASE = "http://recorder:8055"
SRC = BASE + "/fixtures/spacex/launches.json"
SINK = BASE + "/sink"


def fetch(url):
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def transform_and_deliver(**context):
    started = time.time()

    launches = fetch(SRC)
    print(f"[etl] fetched {len(launches)} launches")

    transformed = []
    for launch in launches:
        year = launch.get("launch_year")
        try:
            year_int = int(str(year)[:4])
        except (TypeError, ValueError):
            year_int = None

        mission = launch.get("mission_name") or "UNKNOWN"
        slug = mission.lower().replace(" ", "-")

        row = {
            "flight_number": launch.get("flight_number"),
            "mission_name": mission,
            "mission_slug": slug,
            "launch_year": year,
            "launch_year_int": year_int,
            "is_success": bool(launch.get("launch_success")),
            "rocket_name": (launch.get("rocket") or {}).get("rocket_name"),
        }
        transformed.append(row)
        print(f"[etl] transformed flight {row['flight_number']} -> {row['mission_slug']}")

    elapsed = time.time() - started
    print(f"[etl] transformed {len(transformed)} rows in {elapsed:.3f}s")

    resp = requests.post(SINK, json={"data": transformed}, timeout=30)
    resp.raise_for_status()
    print(f"[etl] delivered {len(transformed)} rows, execution time {elapsed:.3f}s")
    return len(transformed)


with DAG(
    dag_id="reference_t2_etl_spacex",
    default_args={"owner": "harness"},
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    transform_and_deliver_task = PythonOperator(
        task_id="transform_and_deliver",
        python_callable=transform_and_deliver,
    )
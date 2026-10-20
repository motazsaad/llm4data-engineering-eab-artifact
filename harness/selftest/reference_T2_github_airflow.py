"""Reference T2 (ETL) DAG for GitHub — harness self-test artifact.

Hand-written control: fetches the pandas repo fixture, selects fields, computes
derived values, prints debug output, reports total execution time, and delivers
the transformed rows.

T2 requires `full_name` and a minimum of 2 columns, so the control must both
transform the data and preserve the repository's full name.
"""

import time
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

BASE = "http://recorder:8055"
SRC = BASE + "/fixtures/github/repo.json"
SINK = BASE + "/sink"


def fetch(url):
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def transform_and_deliver(**context):
    started = time.time()

    repo = fetch(SRC)
    print(f"[etl] fetched repo: {repo.get('full_name')}")

    stars = repo.get("stargazers_count") or 0
    forks = repo.get("forks_count") or 0
    issues = repo.get("open_issues_count") or 0
    owner = repo.get("owner") or {}

    stars_per_fork = round(stars / forks, 2) if forks else 0

    row = {
        "full_name": repo.get("full_name"),
        "name": repo.get("name"),
        "owner_login": owner.get("login"),
        "stars_per_fork": stars_per_fork,
        "has_open_issues": issues > 0,
        "language": repo.get("language"),
        "created_at": repo.get("created_at"),
    }

    elapsed = time.time() - started
    print(f"[etl] transformed {row['full_name']} in {elapsed:.3f}s")

    resp = requests.post(SINK, json={"data": [row]}, timeout=30)
    resp.raise_for_status()
    print(f"[etl] delivered 1 row, execution time {elapsed:.3f}s")
    return 1


with DAG(
    dag_id="reference_t2_etl_github",
    default_args={"owner": "harness"},
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    transform_and_deliver_task = PythonOperator(
        task_id="transform_and_deliver",
        python_callable=transform_and_deliver,
    )
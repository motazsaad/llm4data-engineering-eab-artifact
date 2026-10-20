"""Reference T1 (ingestion) DAG for GitHub — harness self-test artifact.

Hand-written control: fetches the pandas repo fixture and delivers it to the
sink unchanged. T1 is the simplest task, so a failure here means the harness is
at fault rather than any model.

The required key paths for GitHub T1 are `full_name` and `owner.login` (see
require_keys in prompts.json), so the delivered row must preserve the nested
owner object rather than flattening it.
"""

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


def ingest_and_deliver(**context):
    repo = fetch(SRC)

    delivered = {
        "full_name": repo.get("full_name"),
        "name": repo.get("name"),
        "owner": repo.get("owner"),
        "stargazers_count": repo.get("stargazers_count"),
        "forks_count": repo.get("forks_count"),
        "open_issues_count": repo.get("open_issues_count"),
        "language": repo.get("language"),
        "created_at": repo.get("created_at"),
    }

    resp = requests.post(SINK, json={"data": delivered}, timeout=30)
    resp.raise_for_status()
    return 1


with DAG(
    dag_id="reference_t1_ingest_github",
    default_args={"owner": "harness"},
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    ingest_and_deliver_task = PythonOperator(
        task_id="ingest_and_deliver",
        python_callable=ingest_and_deliver,
    )
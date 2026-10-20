"""Reference T3 (relational combine) DAG — harness self-test artifact.

Hand-written control artifact: performs the join the T3 prompt asks for, using
only the platform's own APIs. If this artifact does not score PASS, the defect is
in the harness, not in any model. Kept in the repo so the measurement instrument
can be validated independently of model quality.
"""

from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

BASE = "http://recorder:8055"
SRC_A = BASE + "/fixtures/github/contributors_pandas.json"
SRC_B = BASE + "/fixtures/github/contributors_scipy.json"
SINK = BASE + "/sink"


def fetch(url):
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def join_and_deliver(**context):
    """Inner-join the two contributor lists on `login`, then deliver."""
    side_a = fetch(SRC_A)
    side_b = fetch(SRC_B)

    by_login = {row["login"]: row for row in side_b}

    joined = []
    for row in side_a:
        other = by_login.get(row["login"])
        if other is None:
            continue
        joined.append({
            "login": row["login"],
            "id": row.get("id"),
            "contributions": row.get("contributions"),
            "contributions_other": other.get("contributions"),
            "html_url": row.get("html_url"),
        })

    resp = requests.post(SINK, json={"data": joined}, timeout=30)
    resp.raise_for_status()
    return len(joined)


default_args = {
    "owner": "harness",
    "retries": 2,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="reference_t3_join",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    join_and_deliver_task = PythonOperator(
        task_id="join_and_deliver",
        python_callable=join_and_deliver,
    )
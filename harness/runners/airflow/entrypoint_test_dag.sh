#!/usr/bin/env bash
# Test a single Airflow DAG headlessly.
# Args: DAG_ID DAG_FILE_PATH [LOGICAL_DATE]
set -uo pipefail

DAG_ID="${1:?usage: entrypoint_test_dag.sh DAG_ID DAG_FILE_PATH [LOGICAL_DATE]}"
DAG_FILE="${2:?missing dag file path}"
DATE="${3:-}"

export AIRFLOW_HOME=/opt/airflow
airflow db migrate >/dev/null 2>&1

# Legacy-compat shim (see ARCHITECTURE.md "Platform compatibility provisioning").
# Airflow 3.x renamed/removed several long-lived public APIs that LLMs very
# often emit from training data (SimpleHttpOperator -> HttpOperator,
# days_ago/weeks_ago moved out of airflow.utils.dates, DummyOperator ->
# EmptyOperator, HttpSensor -> HttpSensor in the http provider). Aliasing them
# keeps a cell's score tied to pipeline logic rather than API-version recall.
SITEC=/opt/airflow/dags_harness_sitecustomize
mkdir -p "$SITEC"
cat > "$SITEC/sitecustomize.py" <<'PY'
import sys, os, types, importlib, functools
from datetime import datetime, timedelta, timezone
from datetime import date

UTC = timezone.utc


def _add(modname, **attrs):
    try:
        m = importlib.import_module(modname)
    except Exception:
        m = sys.modules.get(modname)
        if m is None:
            m = types.ModuleType(modname)
            sys.modules[modname] = m
    for k, v in attrs.items():
        if not hasattr(m, k):
            setattr(m, k, v)
    return m


# --- http provider operator/sensor renames ---------------------------------
try:
    from airflow.providers.http.operators.http import HttpOperator
    _add("airflow.providers.http.operators.http", SimpleHttpOperator=HttpOperator)
    _add("airflow.operators.http", SimpleHttpOperator=HttpOperator)
    try:
        from airflow.providers.http.sensors.http import HttpSensor
        _add("airflow.sensors.http", HttpSensor=HttpSensor)
    except Exception:
        pass
except Exception:
    pass

# --- DummyOperator -> EmptyOperator -----------------------------------------
try:
    from airflow.providers.standard.operators.empty import EmptyOperator
    _add("airflow.operators.dummy", DummyOperator=EmptyOperator)
    _add("airflow.operators.dummy_operator", DummyOperator=EmptyOperator)
    _add("airflow.operators.empty", DummyOperator=EmptyOperator)
except Exception:
    pass

# --- airflow.utils.dates date helpers --------------------------------------
# `datetime` / `date` / `timedelta` were module-level names in this module for
# Airflow 1.x/2.x, so `from airflow.utils.dates import datetime, days_ago` is a
# training-data pattern, not a hallucination.
_add("airflow.utils.dates",
     days_ago=lambda n: datetime.now(UTC) - timedelta(days=n),
     weeks_ago=lambda n: datetime.now(UTC) - timedelta(weeks=n),
     months_ago=lambda n: datetime.now(UTC) - timedelta(days=30 * n),
     datetime=datetime, date=date, timedelta=timedelta)

# --- DAG() keyword compatibility ------------------------------------------
# Airflow 3 dropped the `schedule_interval` / `timetable`-era kwargs in favour
# of `schedule`. Accept the legacy spelling so DAG construction succeeds.
try:
    from airflow.sdk import DAG as _SDAG
    _orig_init = _SDAG.__init__

    def _patched_init(self, *args, **kwargs):
        if "schedule_interval" in kwargs:
            kwargs.setdefault("schedule", kwargs.pop("schedule_interval"))
        # Models commonly put retry policy in DAG-level default_args (often with
        # retry_exponential_backoff), where the operator-level clamp cannot see
        # it. Clamp here too, otherwise a cell can burn 35+ minutes of real time.
        da = kwargs.get("default_args")
        if isinstance(da, dict):
            kwargs["default_args"] = dict(da)
            _clamp_retry_delays(kwargs["default_args"])
        return _orig_init(self, *args, **kwargs)

    _SDAG.__init__ = _patched_init
except Exception:
    pass

# --- common legacy operator-module aliases ---------------------------------
# Pre-3.0 module paths that models emit verbatim from training data. Each maps
# to its current provider location; only the public names are re-exported.
for _legacy, _new in [
    ("airflow.operators.python_operator", "airflow.providers.standard.operators.python"),
    ("airflow.operators.python", "airflow.providers.standard.operators.python"),
    ("airflow.operators.bash_operator", "airflow.providers.standard.operators.bash"),
    ("airflow.operators.bash", "airflow.providers.standard.operators.bash"),
    ("airflow.operators.empty", "airflow.providers.standard.operators.empty"),
    ("airflow.operators.datetime", "airflow.providers.standard.operators.datetime"),
    # Airflow 1.x path for SimpleHttpOperator; the harness aliases the class
    # onto both this module and `airflow.operators.http` below.
    ("airflow.operators.http_operator", "airflow.providers.http.operators.http"),
    ("airflow.sensors.python_sensor", "airflow.providers.standard.sensors.python"),
    ("airflow.sensors.date_time_sensor", "airflow.providers.standard.sensors.date_time"),
    ("airflow.sensors.external_task_sensor", "airflow.providers.standard.sensors.external_task"),
    ("airflow.sensors.time_delta_sensor", "airflow.providers.standard.sensors.time_delta"),
    ("airflow.hooks.base", "airflow.hooks.base"),
]:
    try:
        _real = importlib.import_module(_new)
        _add(_legacy, **{k: v for k, v in vars(_real).items() if not k.startswith("_")})
    except Exception:
        pass

# --- removed/renamed operator kwargs ---------------------------------------
# `provide_context` was a real PythonOperator argument in Airflow 1.x/2.0 and is
# gone in Airflow 3 (context is always provided); `task_concurrency` became
# `max_active_tis_per_dag`. Models emit both verbatim from training data, and a
# DAG that fails to *parse* would otherwise be scored as a reasoning failure.
# Both are accepted as no-ops / direct renames, identically for every cell.
# --- bound retry delays -----------------------------------------------------
# `airflow dags test` waits in *real* time, so a model-chosen
# `retries=3, retry_delay=5min` costs 15 minutes of wall clock and an injected
# 503 can never be observed retrying within any sane budget. Left unbounded this
# makes the harness's own retry criterion unmeasurable for a large share of
# models, which would bias the study. Retry *counts* and retry behavior are
# preserved; only the delay is clamped, and every clamp is logged.
_MAX_RETRY_DELAY_S = float(os.environ.get("HARNESS_MAX_RETRY_DELAY_S", "10"))


def _clamp_retry_delays(kwargs):
    for key in ("retry_delay", "max_retry_delay"):
        val = kwargs.get(key)
        if isinstance(val, timedelta):
            secs = val.total_seconds()
            if secs > _MAX_RETRY_DELAY_S:
                print(f"[harness] clamped {key} {val} -> {_MAX_RETRY_DELAY_S}s",
                      flush=True)
                kwargs[key] = timedelta(seconds=_MAX_RETRY_DELAY_S)


try:
    from airflow.providers.standard.operators.python import (PythonOperator,
                                                             PythonVirtualenvOperator,
                                                             BranchPythonOperator)

    def _patch_removed_kwargs(cls):
        _orig = cls.__init__

        @functools.wraps(_orig)
        def _init(self, *args, **kwargs):
            kwargs.pop("provide_context", None)
            if "task_concurrency" in kwargs:
                kwargs.setdefault("max_active_tis_per_dag",
                                  kwargs.pop("task_concurrency"))
            _clamp_retry_delays(kwargs)
            return _orig(self, *args, **kwargs)

        cls.__init__ = _init

    for _cls in (PythonOperator, PythonVirtualenvOperator, BranchPythonOperator):
        _patch_removed_kwargs(_cls)
except Exception:
    pass

try:
    from airflow.sdk import BaseOperator as _BOp

    _bop_orig = _BOp.__init__

    @functools.wraps(_bop_orig)
    def _bop_init(self, *args, **kwargs):
        kwargs.pop("provide_context", None)
        if "task_concurrency" in kwargs:
            kwargs.setdefault("max_active_tis_per_dag",
                              kwargs.pop("task_concurrency"))
        _clamp_retry_delays(kwargs)
        return _bop_orig(self, *args, **kwargs)

    _BOp.__init__ = _bop_init
except Exception:
    pass

# --- airflow.hooks.base kept a stable path; alias the moved SQL hook --------
try:
    from airflow.providers.common.sql.hooks.sql import DbApiHook  # noqa: F401
    _add("airflow.hooks.sql_hook", DbApiHook=DbApiHook)
    _add("airflow.hooks.base", DbApiHook=DbApiHook)
except Exception:
    pass
PY
export PYTHONPATH="$SITEC:${PYTHONPATH:-}"

# --- Provision the recorder connection --------------------------------------
# Generated DAGs may route calls through a named HTTP connection
# (http_conn_id=...). The harness pre-registers one HTTP connection per
# conn_id referenced by the artifact, pointing at the recorder base URL, so
# connection setup is uniform environment provisioning rather than a
# per-model manual step. Uniform across all cells (see ARCHITECTURE.md).
RECORDER_BASE="${RECORDER_BASE:-http://recorder:8055}"
python3 - "$DAG_FILE" "$RECORDER_BASE" <<'PY'
import os, re, sys
from urllib.parse import urlparse

dag_file, base = sys.argv[1], sys.argv[2]
try:
    from airflow.models import Connection
    from airflow.settings import Session
except Exception:
    sys.exit(0)

src = open(dag_file, encoding="utf-8", errors="ignore").read()


def const(name):
    m = re.search(rf"\b{re.escape(name)}\b\s*(?::[^=\n]+)?=\s*[\"']([^\"']+)[\"']", src)
    return m.group(1) if m else None


# A literal `http_conn_id="x"` is the common case, but models also write
# `SINK_CONN_ID = "recorder_sink_api"; ... http_conn_id=SINK_CONN_ID`. Missing
# the constant form means the harness provisions nothing and the run dies with
# "The conn_id `x` isn't defined" -- a provisioning gap, not a model failure.
conn_ids = set(re.findall(r"http_conn_id\s*=\s*[\"']([^\"']+)[\"']", src))
for m in re.finditer(r"http_conn_id\s*=\s*([A-Za-z_]\w*)", src):
    val = const(m.group(1))
    if val:
        conn_ids.add(val)

u = urlparse(base)
existing = {c.conn_id for c in Session().query(Connection).all()}
for cid in sorted(conn_ids):
    if cid in existing:
        continue
    Session().add(Connection(
        conn_id=cid, conn_type="http",
        host=u.hostname or "recorder", port=u.port or 80,
        schema=u.scheme or "http",
    ))
Session().commit()
print(f"[harness] provisioned {len(conn_ids)} http connection(s): {sorted(conn_ids)}")
PY

if [[ -n "$DATE" ]]; then
  airflow dags test "$DAG_ID" "$DATE" -f "$DAG_FILE"
else
  airflow dags test "$DAG_ID" -f "$DAG_FILE"
fi

echo "DAG_EXIT_CODE=$?"
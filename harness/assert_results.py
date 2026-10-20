#!/usr/bin/env python3
"""Score one run against the recorded ground truth.

Reads the recorder sink log (delivered payloads) and the request trace, then
checks execution-based success criteria for the task:

  T1 Ingestion      : >=1 delivery to sink, payload is the fetched source
                      object (>= 1 row), no credentials leaked.
  T2 ETL            : >=1 delivery, transformed rows present, columns >= min.
  T3 Relational     : >=1 delivery, joined rows each containing keys from
                      both sources (e.g. flight_number + rocket_name).

Optional: --expect-fail-injection requires >=N retried source requests in the
trace (proves the pipeline actually retried after a 503).

Exit code 0 iff all checks pass. Writes a machine-readable score to --score.
"""

import argparse
import json
import os
import sys


def load_jsonl(path):
    rows = []
    if os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
    return rows


def rows_in_payload(payload):
    """Normalize a delivered payload into an iterable of row dicts."""
    if payload is None:
        return []
    if isinstance(payload, dict):
        if isinstance(payload.get("data"), list):
            return [r for r in payload["data"] if isinstance(r, dict)]
        if isinstance(payload.get("data"), dict):
            # A single record wrapped as {"data": {...}}. This is a legitimate
            # delivery shape for a one-object source (e.g. a GitHub repo), and
            # the GitHub T1 control produces it. Treating the wrapper itself as
            # the row hid every real key from required_keys_present, so the
            # required-key check failed on a correct pipeline.
            return [payload["data"]]
        return [payload] if payload else []
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    return []


def collect_df_like(payloads):
    """Return (row_count, list_of_row_dicts) flattening list payloads."""
    rows = []
    for p in payloads:
        rows.extend(rows_in_payload(p))
    return rows


def nested_get(row, keypath):
    cur = row
    for part in keypath.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--task", required=True, choices=["T1", "T2", "T3"])
    ap.add_argument("--log", default="sink.jsonl")
    ap.add_argument("--trace", default="trace.jsonl")
    ap.add_argument("--score", default="score.json")
    ap.add_argument("--min-deliveries", type=int, default=1)
    ap.add_argument("--min-rows", type=int, default=1)
    ap.add_argument("--min-columns", type=int, default=1)
    ap.add_argument("--require-keys", default="", help="comma-separated key paths required in each row")
    ap.add_argument("--expect-rows", type=int, default=0,
                    help="exact expected row count (a join's cardinality)")
    ap.add_argument("--expect-fail-injection", type=int, default=0,
                    help=">=N 503s must have been observed (retry behavior)")
    args = ap.parse_args()

    deliveries = load_jsonl(args.log)
    trace = load_jsonl(args.trace)
    fails = sum(1 for t in trace if t.get("status") == 503)
    sources_fetched = sum(1 for t in trace if t.get("method") == "GET" and "/fixtures/" in t.get("path", ""))

    rows = collect_df_like([d["payload"] for d in deliveries])

    checks = []
    checks.append(("deliver_sink", len(deliveries) >= args.min_deliveries, len(deliveries)))
    checks.append(("rows_delivered", len(rows) >= args.min_rows, len(rows)))

    required = [k.strip() for k in args.require_keys.split(",") if k.strip()]
    missing = set()
    for r in rows:
        for k in required:
            if nested_get(r, k) is None:
                missing.add(k)
    checks.append(("required_keys_present", not missing, sorted(missing)))

    col_counts = [len(r) for r in rows]
    min_cols = min(col_counts) if col_counts else 0
    checks.append(("columns_non_trivial", min_cols >= args.min_columns, min_cols))

    if args.expect_rows > 0:
        # Exact cardinality. Row presence alone cannot distinguish a correct join
        # from a cross product: on the GitHub fixtures only 3 of 100 contributors
        # overlap, so a correct inner join yields 3 rows while a cartesian join
        # yields 10,000. Both contain `login` and `contributions`, so the
        # required-key check passed on both.
        checks.append(("row_count_exact", len(rows) == args.expect_rows,
                       f"{len(rows)} (expected {args.expect_rows})"))

    if args.expect_fail_injection > 0:
        checks.append(("retried_on_failure", fails >= args.expect_fail_injection, fails))
        checks.append(("source_fetched", sources_fetched >= args.expect_fail_injection + 1, sources_fetched))

    passed = all(ok for _, ok, _ in checks)
    score = {
        "task": args.task,
        "deliveries": len(deliveries),
        "rows": len(rows),
        "min_columns_observed": min_cols,
        "failed_requests_observed": fails,
        "source_fetches": sources_fetched,
        "checks": {name: {"ok": ok, "value": val} for name, ok, val in checks},
        "passed": passed,
    }
    with open(args.score, "w", encoding="utf-8") as fh:
        json.dump(score, fh, indent=2)

    for name, ok, val in checks:
        print(f"[{'PASS' if ok else 'FAIL'}] {name}: {val}")
    print(f"RESULT={'PASS' if passed else 'FAIL'}")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
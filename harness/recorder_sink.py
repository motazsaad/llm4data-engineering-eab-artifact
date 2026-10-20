#!/usr/bin/env python3
"""Hermetic recorder sink + fixture source for the LLM4Data validation harness.

Serves:
  GET  /fixtures/<file>            -> fixture JSON from fixtures/ (hermetic source)
  POST /sink                       -> records the delivered payload (JSONL)
  GET  /__control/ready            -> {"ok": true} readiness probe
  POST /__control/fail?times=N     -> next N source GETs return 503 (retry test)

Every recorded delivery is appended as one JSON line to the sink log. This is the
ground truth the assertion script reads (row counts, column sets, delivery).
"""

import argparse
import json
import os
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

LOCK = threading.Lock()
FAIL_REMAINING = 0


def now_iso():
    return datetime.now(timezone.utc).isoformat()


class ControlState:
    def __init__(self, log_path, trace_path):
        self.log_path = log_path
        self.trace_path = trace_path
        self.post_count = 0
        self.fail_remaining = 0
        self.fails_served = 0

    def trace(self, method, path, status):
        with LOCK:
            with open(self.trace_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps({"ts": now_iso(), "method": method, "path": path, "status": status}) + "\n")

STATE = None


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _text(self, code, body):
        data = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _json(self, code, obj):
        self._text(code, json.dumps(obj))

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/__control/ready":
            STATE.trace("GET", parsed.path, 200)
            self._json(200, {"ok": True})
            return
        if parsed.path == "/__control/stats":
            STATE.trace("GET", parsed.path, 200)
            self._json(200, {"posts": STATE.post_count, "fails_served": STATE.fails_served,
                             "fail_remaining": STATE.fail_remaining})
            return
        if parsed.path.startswith("/fixtures/"):
            rel = parsed.path[len("/fixtures/"):]
            fixture_dir = FIXTURE_DIR
            fpath = os.path.normpath(os.path.join(fixture_dir, rel))
            if not fpath.startswith(os.path.normpath(fixture_dir)):
                STATE.trace("GET", parsed.path, 404)
                self._json(404, {"error": "invalid fixture path"})
                return
            with LOCK:
                fail_now = STATE.fail_remaining > 0
                if fail_now:
                    STATE.fail_remaining -= 1
                    STATE.fails_served += 1
            if fail_now:
                time.sleep(0.1)
                STATE.trace("GET", parsed.path, 503)
                self._json(503, {"error": "injected failure"})
                return
            if not os.path.isfile(fpath):
                STATE.trace("GET", parsed.path, 404)
                self._json(404, {"error": f"fixture not found: {rel}"})
                return
            with open(fpath, encoding="utf-8") as fh:
                try:
                    payload = json.load(fh)
                except Exception as exc:
                    STATE.trace("GET", parsed.path, 500)
                    self._json(500, {"error": f"bad fixture json: {exc}"})
                    return
            STATE.trace("GET", parsed.path, 200)
            self._json(200, payload)
            return
        # Trace it: an untraced 404 hides a request that the node actually sent
        # (e.g. an HTTP Request node that fell back to GET because the harness
        # or the model named the method parameter wrong).
        STATE.trace("GET", parsed.path, 404)
        self._json(404, {"error": "not found", "path": parsed.path})

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/__control/fail":
            times = parse_qs(parsed.query).get("times", ["1"])[0]
            int_times = max(1, int(times))
            with LOCK:
                STATE.fail_remaining = max(STATE.fail_remaining, int_times)
            STATE.trace("POST", parsed.path, 200)
            self._json(200, {"fail_remaining": STATE.fail_remaining})
            return
        if parsed.path == "/__control/reset":
            with LOCK:
                STATE.post_count = 0
                STATE.fail_remaining = 0
                STATE.fails_served = 0
            for p in (STATE.log_path, STATE.trace_path):
                if os.path.exists(p):
                    os.remove(p)
            STATE.trace("POST", parsed.path, 200)
            self._json(200, {"reset": True})
            return

        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length).decode("utf-8", "replace") if length else ""
        try:
            payload = json.loads(body) if body else None
        except json.JSONDecodeError:
            payload = {"__raw": body}
        record = {
            "ts": now_iso(),
            "path": parsed.path,
            "content_type": self.headers.get("Content-Type", ""),
            "payload": payload,
        }
        with LOCK:
            STATE.post_count += 1
            with open(STATE.log_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        STATE.trace("POST", parsed.path, 200)
        self._json(200, {"received": True, "count": STATE.post_count})


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8055)
    ap.add_argument("--fixtures-dir", default=os.path.join(os.path.dirname(__file__), "fixtures"))
    ap.add_argument("--log", default="sink.jsonl")
    ap.add_argument("--trace", default="trace.jsonl")
    args = ap.parse_args()

    global STATE, FIXTURE_DIR
    FIXTURE_DIR = args.fixtures_dir
    STATE = ControlState(args.log, args.trace)

    server = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    print(f"recorder_sink listening on :{args.port}, fixtures={FIXTURE_DIR}, log={args.log}, trace={args.trace}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
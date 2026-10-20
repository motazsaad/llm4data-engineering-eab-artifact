#!/usr/bin/env python3
"""Extract concise, actionable repair feedback from a failed cell.

Keeps only: the essential error lines (traceback / SyntaxError / TypeError /
n8n JSON errors), the failing checks, and actual-vs-expected metrics. Warnings
and deprecation noise are dropped so the feedback stays small and the model
spends its budget producing the corrected artifact, not re-reading logs.
"""
import json
import re
import sys
from pathlib import Path

WARN = re.compile(r"\b(warning|deprecat|INFO|\[\s*info\s*\])\b", re.I)
ERRKEYS = ("SyntaxError", "TypeError", "NameError", "AttributeError",
           "ImportError", "ModuleNotFoundError", "ValueError", "KeyError",
           "JSONDecodeError", "NodeOperationError", "Error executing",
           "Unrecognized node type", "Execution error", "Error:",
           "needs to be valid JSON", "could not be found", "failed to parse",
           "DAG_EXIT_CODE", "Traceback (most recent call last)")


def essential_errors(runlog: Path, max_lines=25, max_chars=1500):
    try:
        text = runlog.read_text(errors="replace")
    except Exception:
        return ""
    raw = text.splitlines()
    keep = set()
    for i, ln in enumerate(raw):
        if WARN.search(ln) and not any(k in ln for k in ERRKEYS):
            continue
        if any(k in ln for k in ERRKEYS):
            for j in range(max(0, i - 2), min(len(raw), i + 1)):
                if not (WARN.search(raw[j]) and not any(k in raw[j] for k in ERRKEYS)):
                    keep.add(j)
    out = [raw[i].rstrip() for i in sorted(keep)]
    # de-dup adjacent and keep the tail
    dedup = []
    for ln in out:
        if not dedup or dedup[-1] != ln:
            dedup.append(ln)
    joined = "\n".join(dedup[-max_lines:])
    return joined[-max_chars:]


def extract_feedback(cell_dir: Path):
    cell_dir = Path(cell_dir)
    fb = {"error_excerpt": "", "checks_failed": {}, "actual_vs_expected": {}}

    fb["error_excerpt"] = essential_errors(cell_dir / "run.log")

    try:
        sc = json.loads((cell_dir / "score.json").read_text())
    except Exception:
        return fb

    fb["actual_vs_expected"] = {
        "deliveries": sc.get("deliveries"),
        "rows": sc.get("rows"),
        "min_columns_observed": sc.get("min_columns_observed"),
        "source_fetches": sc.get("source_fetches"),
    }
    for name, c in (sc.get("checks") or {}).items():
        if not c.get("ok"):
            fb["checks_failed"][name] = c.get("value")
    return fb


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: repair_feedback.py <cell_dir>", file=sys.stderr)
        sys.exit(1)
    print(json.dumps(extract_feedback(Path(sys.argv[1])), indent=2))

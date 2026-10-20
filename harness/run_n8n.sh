#!/usr/bin/env bash
# Import and execute one n8n workflow artifact against the recorder.
# Runs a single n8n container: initialises the DB + owner, imports the
# workflow, reads its id, then executes it.
# Args: ARTIFACT_PATH [RECORDER_BASE]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [[ $# -lt 1 ]]; then
  echo "usage: run_n8n.sh ARTIFACT_PATH [RECORDER_BASE]" >&2
  exit 2
fi
ARTIFACT="$(readlink -f "$1")"
RECORDER_BASE="${2:-http://recorder:8055}"
NET="$(docker network ls --format '{{.Name}}' | grep -E 'harness' | head -1)"
if [[ -z "$NET" ]]; then
  echo "no harness network found; run: docker compose up -d recorder" >&2
  exit 2
fi

FNAME="$(basename "$ARTIFACT")"
STAMP="$(date -u +%Y%m%d-%H%M%S)"
NF="out/n8n_${STAMP}"
DATA="$SCRIPT_DIR/$NF/data"
rm -rf "$SCRIPT_DIR/$NF"
mkdir -p "$DATA"
chmod 777 "$SCRIPT_DIR/$NF" "$DATA"

# Normalize the workflow JSON before import:
#   * n8n's importer requires a non-null `active` flag and an owning user;
#     LLMs routinely omit `active`.
#   * `n8n execute` can only start a workflow from a trigger the CLI can fire
#     headlessly. Models routinely emit schedule/cron/webhook triggers, which
#     either wait for a schedule or need a live server, so we replace the
#     model's start trigger with a Manual Trigger wired to the same downstream
#     node.
# Neither fix alters the pipeline's data-flow logic, both are applied
# identically to every cell, and both are documented in ARCHITECTURE.md §4.
NORM="$SCRIPT_DIR/$NF/$FNAME"
# Unique per-import suffix so pick.js can identify this run's workflow even when
# the n8n data dir already holds an identically named workflow from an earlier run.
export HARNESS_RUN_ID="$NF"
python3 - "$ARTIFACT" "$NORM" <<'PY'
import json, os, re, sys

src, dst = sys.argv[1], sys.argv[2]
wf = json.load(open(src, encoding="utf-8"))
if isinstance(wf, list):
    wf = wf[0]

wf.setdefault("active", False)
wf.setdefault("settings", {})
wf.setdefault("connections", {})
wf.setdefault("nodes", [])
# The name is made unique per import. The workflow id is recovered by matching
# on name (pick.js), and the n8n data directory is shared across runs, so two
# runs of the same cell that keep the model's own workflow name would otherwise
# match an *earlier* run's workflow and silently execute stale logic -- which
# looks exactly like a model failure (empty execution, exit 0).
_uniq = os.environ.get("HARNESS_RUN_ID") or "run"
wf["name"] = "{0} [harness:{1}]".format(wf.get("name") or "harness-workflow", _uniq)

# Triggers the headless CLI can actually start from.
CLI_TRIGGERS = {
    "n8n-nodes-base.manualTrigger",
    "n8n-nodes-base.executeWorkflowTrigger",
}

# Lowest node version still shipped in the pinned n8n image. Models routinely
# emit versions that were valid in older n8n releases (e.g. `if` v1.1), which
# the engine cannot load at all. When -- and only when -- the requested version
# is below the minimum, we bump it to that minimum. Every bump is logged.
# Only nodes whose older versions are still loadable are floored. `merge` is
# deliberately absent: Merge v3 is a redesign (explicit combination modes), not a
# compatible bump, and forcing a v1 Merge to v3 yields a workflow n8n cannot
# instantiate ("Cannot read properties of undefined (reading 'execute')",
# lastNodeExecuted = "Merge Data"). That is a worse outcome than letting the
# model's own version stand and failing for its actual reason.
MIN_NODE_VERSION = {
    "n8n-nodes-base.if": 2,
    "n8n-nodes-base.switch": 3,
    # Merge v1 is gone from the pinned image and v3 is a redesign with explicit
    # combination modes; forcing v1 -> v3 left n8n unable to instantiate the node.
    # v2 ("Combine") keeps the positional-merge semantics a v1 author meant.
    "n8n-nodes-base.merge": 2,
    "n8n-nodes-base.splitInBatches": 3,
    "n8n-nodes-base.set": 3.4,
    "n8n-nodes-base.aggregate": 1,
    "n8n-nodes-base.sort": 1,
    "n8n-nodes-base.limit": 1,
    "n8n-nodes-base.filter": 2.2,
    "n8n-nodes-base.removeDuplicates": 2,
    "n8n-nodes-base.dateTime": 2,
    "n8n-nodes-base.html": 1.2,
    "n8n-nodes-base.httpRequest": 4.1,
    "n8n-nodes-base.code": 2,
    "n8n-nodes-base.noOp": 1,
    "n8n-nodes-base.stickyNote": 1,
}


def vtuple(v):
    try:
        return tuple(float(x) for x in str(v).split("."))
    except (TypeError, ValueError):
        return None


remapped = []
for n in wf["nodes"]:
    want = n.get("typeVersion")
    floor = MIN_NODE_VERSION.get(n.get("type"))
    if floor is None or want is None:
        continue
    wv, fv = vtuple(want), vtuple(floor)
    if wv is not None and fv is not None and wv < fv:
        remapped.append(f"{n['type']} v{want}->v{floor}")
        n["typeVersion"] = floor
if remapped:
    print(f"[harness] node version remap: {', '.join(remapped)}")

# Renamed parameters. Models routinely emit the n8n v3 field name for the JSON
# request body; the pinned image (>=1.80) uses `jsonBody`. Without this rename
# the node fails with "JSON parameter needs to be valid JSON" — the model's
# intent is unambiguous, and this is version drift, not a data-flow error, so it
# falls under the same compatibility policy as the Airflow legacy kwargs.
# Request-body parameter drift. In v2 an HTTP Request node kept the JSON body in
# `bodyParametersJson`, gated by a boolean `jsonParameters` flag; v4 folds the
# body into `jsonBody` and expects `sendBody`/`specifyBody` to enable it.
#
# These are separate concerns, so they must not be handled by one rename table:
# treating `jsonParameters: true` as the body sets `jsonBody` to a boolean, which
# n8n rejects with "Data after transformation must be a string". Map only the
# body field, drop the boolean flag, then enable the body explicitly.
BODY_PARAM_OLD = ("bodyParametersJson",)
BODY_FLAG_OLD = ("jsonParameters",)
renamed_params = []
for n in wf["nodes"]:
    p = n.get("parameters")
    if not isinstance(p, dict):
        continue
    if not n.get("type", "").endswith("httpRequest"):
        continue

    for old in BODY_PARAM_OLD:
        if old in p and "jsonBody" not in p:
            p["jsonBody"] = p.pop(old)
            renamed_params.append(f"{n.get('name')}: {old}->jsonBody")

    # The v2 boolean is subsumed by sendBody/specifyBody below.
    for old in BODY_FLAG_OLD:
        if old in p:
            p.pop(old)
            renamed_params.append(f"{n.get('name')}: dropped {old} flag")

    if "jsonBody" in p:
        if p.get("sendBody") is not True:
            p["sendBody"] = True
            renamed_params.append(f"{n.get('name')}: sendBody->true")
        if p.get("specifyBody") != "json":
            p["specifyBody"] = "json"
            renamed_params.append(f"{n.get('name')}: specifyBody->json")
if renamed_params:
    print(f"[harness] param rename: {', '.join(renamed_params)}")

# Removed node types -> their successors. The Function node was deprecated and
# then removed from n8n in favour of the Code node; a workflow that still uses
# it does not import, and the CLI reports the confusing
# "Cannot read properties of undefined (reading 'execute')". Models emit
# `n8n-nodes-base.function` constantly, and it was valid when they learned it, so
# this is platform drift rather than a model error -- the same category as the
# Airflow legacy kwargs.
NODE_TYPE_REMAP = {"n8n-nodes-base.function": "n8n-nodes-base.code"}
retyped = []
for n in wf["nodes"]:
    new_type = NODE_TYPE_REMAP.get(n.get("type"))
    if new_type is None:
        continue
    n["type"] = new_type
    n["typeVersion"] = 2
    pr = n.setdefault("parameters", {})
    # Models spell the body key either way: older exports use `functionItem`,
    # newer ones `functionCode`.
    body = pr.pop("functionItem", None) or pr.pop("functionCode", None)
    if body is not None and not pr.get("jsCode"):
        # Function bodies address the input as `items`; in the Code node it is
        # $input.all(). The rest of the body (including `return [{json: ...}]`)
        # is already Code-compatible. Models that already declare `items`
        # themselves must not get a second `const items` prepended: that is a
        # duplicate declaration, which the Code node rejects outright with
        # "Identifier 'items' has already been declared" and would otherwise be
        # scored as a model failure. Comments are stripped first so that a
        # commented-out declaration still counts as undeclared.
        declared = re.sub(r"/\*.*?\*/|(?<!:)//[^\n]*", "", body, flags=re.S)
        if not re.search(r"\b(?:const|let|var)\s+items\s*=", declared):
            body = "const items = $input.all();\n" + body
        pr["jsCode"] = body
    retyped.append(f"{n.get('name')}: function->code")
if retyped:
    print(f"[harness] node type remap: {', '.join(retyped)}")


def edges(conns):
    """Flatten n8n connection map to {(source, branch): [target, ...]}."""
    out = {}
    for src_name, outs in conns.items():
        if not isinstance(outs, dict):
            continue
        for branch, groups in outs.items():
            if not isinstance(groups, list):
                continue
            flat = []
            for group in groups:
                if isinstance(group, list):
                    flat += [c.get("node") for c in group if isinstance(c, dict)]
            out[(src_name, branch)] = flat
    return out


nodes = wf["nodes"]
has_cli_trigger = any(n.get("type") in CLI_TRIGGERS for n in nodes)

# Non-CLI trigger node types: scheduled / event / webhook starts. Identified by
# type, not by graph position, because models emit them inconsistently and
# sometimes wire nothing at all.
TRIGGER_HINTS = ("trigger", "cron", "webhook", "interval", "schedule")


def is_non_cli_trigger(n):
    t = (n.get("type") or "").lower()
    return any(h in t for h in TRIGGER_HINTS) and n.get("type") not in CLI_TRIGGERS


if not has_cli_trigger and nodes:
    conns = wf["connections"]
    e = edges(conns)

    start = next((n for n in nodes if is_non_cli_trigger(n)), None)
    is_real_trigger = start is not None
    if start is None:
        # The model wired no trigger at all. Its entry node is real work (often
        # the HTTP fetch), so it must be kept: we insert a Manual Trigger in
        # FRONT of it rather than replacing it. An earlier version treated the
        # entry node as the trigger and deleted it, which silently turned every
        # such workflow into a bare sink POST with zero rows -- an invalidating
        # harness defect, since it scored correct models as failures.
        downstream = {t for tgts in e.values() for t in tgts}
        start = next((n for n in nodes if n["name"] not in downstream), nodes[0])

    # Prefer the branch the model wired the first hop on ("main": [[...]]).
    branch = next((b for (s, b), tgts in e.items()
                   if s == start["name"] and tgts), "main")
    targets = e.get((start["name"], branch), [])
    if not targets:
        targets = [t for (s, _), tgts in e.items() if s == start["name"] for t in tgts]

    trigger = {
        "parameters": {},
        "id": "harness-manual-trigger",
        "name": "Manual Trigger",
        "type": "n8n-nodes-base.manualTrigger",
        "typeVersion": 1,
        "position": [0, 0],
    }
    if is_real_trigger:
        # Drop the model's non-CLI trigger: it cannot be fired headlessly, and
        # leaving it in place only makes the engine fail to load the node.
        wf["nodes"] = [n for n in nodes if n["name"] != start["name"]]
        conns.pop(start["name"], None)
        conns["Manual Trigger"] = {"main": [[{"node": t, "type": "main", "index": 0}
                                            for t in targets]]}
        print(f"[harness] replaced trigger {start.get('type')} with Manual Trigger "
              f"-> {targets or ['(no target)']}")
    else:
        # No trigger to drop: prepend one and point it at the model's entry node,
        # preserving every connection the model authored.
        wf["nodes"] = [trigger] + list(nodes)
        conns["Manual Trigger"] = {"main": [[{"node": start["name"],
                                              "type": "main", "index": 0}]]}
        print(f"[harness] inserted Manual Trigger before entry node "
              f"{start['name']!r} (model emitted no trigger; graph preserved)")

json.dump(wf, open(dst, "w", encoding="utf-8"), indent=2)
print(f"[harness] normalized workflow: nodes={len(wf['nodes'])} active={wf['active']}")
PY

cat > "$SCRIPT_DIR/$NF/pick.js" <<'PICKJS'
// Picks the id of the workflow we just imported.
// export:workflow --all also returns any other workflow in the mounted data dir
// (n8n seeds a workflow-example on first start). Taking a[0] executed the wrong
// workflow, yielding an empty execution that looked like a model failure.
const fs = require("fs");
const w = JSON.parse(fs.readFileSync("/out/exported.json", "utf8"));
const a = Array.isArray(w) ? w : [w];
const want = process.env.WF_NAME || "";
const m = a.find((x) => x && x.name === want)
       || a.find((x) => x && x.id !== "workflow-example")
       || a[0];
console.log(m && m.id ? m.id : "");
PICKJS
WF_NAME=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1])).get("name",""))' "$SCRIPT_DIR/$NF/$(basename "$NORM")" 2>/dev/null || echo "")
echo "n8n importing + executing $FNAME (recorder=$RECORDER_BASE, name=$WF_NAME)"
set +e
docker run --rm \
  --network "$NET" \
  --user 1000:1000 \
  --entrypoint "" \
  -e N8N_USER_FOLDER=/home/node/.n8n \
  -e HOME=/home/node \
  -e N8N_USER_MANAGEMENT_DISABLED=true \
  -e N8N_DIAGNOSTICS_ENABLED=false \
  -e N8N_ENFORCE_SETTINGS_FILE_PERMISSIONS=false \
  -e GENERIC_TIMEZONE=UTC \
  -e N8N_ENCRYPTION_KEY=harness-local-encryption-key \
  -e RECORDER_BASE="$RECORDER_BASE" \
  -e N8N_EXEC_ATTEMPTS="${N8N_EXEC_ATTEMPTS:-5}" \
  -e WF_NAME="$WF_NAME" \
  -e HARNESS_RUN_ID="$NF" \
  -v "$SCRIPT_DIR/$NF":/out \
  -v "$DATA":/home/node/.n8n \
  -w /home/node \
  n8nio/n8n:1.90.1 \
  sh -c "
    # Create the owner user (user-management is disabled -> single default user).
    n8n user-management:reset --email=harness@local --password=harnesslocal > /out/user.log 2>&1 || true
    n8n import:workflow --input=/out/$(basename "$NORM") > /out/import.json 2>&1
    # The importer does not echo the created workflow id on success; export
    # the stored workflow back out and take its id from the JSON.
    n8n export:workflow --all --output=/out/exported.json > /out/export.log 2>&1
    # Select by name, not position. export:workflow --all also returns other
    # workflows in the mounted data dir (n8n seeds a workflow-example on first
    # start); taking a[0] silently executed the WRONG workflow, producing an
    # empty execution that then looked like a model failure.
    WID=\$(node /out/pick.js 2>/dev/null) || WID=''
    if [ -z \"\$WID\" ]; then echo WORKFLOW_ID_MISSING >> /out/import.json; exit 2; fi
    echo \"ID=\$WID\" > /out/import.log

    # Transport-error retry.
    #
    # Measured on this pinned image: a hand-written control workflow that fetches
    # two sources and delivers (selftest/reference_T3_github_n8n.json) succeeds
    # only 4/8 times, while the *identical* workflow on Airflow succeeds 8/8. The
    # recorder serves the failing request with HTTP 200 every time (observed >90
    # retries for a single logical request) -- n8n's HTTP client discards the
    # response and its own pooled socket is left unusable. That is a property of
    # the platform, not of the artifact, so it must not be charged to the model.
    #
    # Only this class of error is retried; anything else (bad node config, wrong
    # expression, model payload bug) fails immediately and is scored normally.
    # Each attempt starts from a clean sink so the scored state belongs to the
    # attempt that actually delivered.
    ATTEMPTS=\${N8N_EXEC_ATTEMPTS:-3}
    i=1
    rc=1
    while [ \$i -le \$ATTEMPTS ]; do
      node -e 'fetch(process.env.RECORDER_BASE+\"/__control/reset\",{method:\"POST\"}).catch(()=>{})' >/dev/null 2>&1 || true
      n8n execute --id=\"\$WID\" --rawOutput > /out/execution.json 2>&1
      rc=\$?
      if [ \$rc -eq 0 ]; then
        echo \"TRANSPORT_RETRIES=\$((i-1))\" > /out/retries.log
        break
      fi
      if ! grep -qiE \"connection to the server was closed unexpectedly|socket hang up|ECONNRESET|ECONNREFUSED|aborted|read ECONNRESET\" /out/execution.json; then
        echo \"TRANSPORT_RETRIES=\$((i-1))\" > /out/retries.log
        break
      fi
      echo \"[harness] n8n transport error on attempt \$i/\$ATTEMPTS, retrying\" >> /out/retries.log
      i=\$((i+1))
    done
    exit \$rc
  " >> "$NF/container.log" 2>&1
EXIT=$?
set -e
echo "n8n container exit: $EXIT"
echo "import log:"; cat "$NF/import.log" 2>/dev/null || true
echo "import (tail):"; tail -5 "$NF/import.json" 2>/dev/null || true
echo "execution (tail):"; tail -15 "$NF/execution.json" 2>/dev/null || true
echo "transport retries:"; cat "$NF/retries.log" 2>/dev/null || echo "  none"
# Persist the retry count next to the cell result so aggregation can read it.
# Historically this stayed only in the n8n workspace, so downstream tooling saw
# no per-cell count and mis-scored every n8n pass as a non-first-attempt pass.
if [[ -n "${HARNESS_RESULT_DIR:-}" && -f "$NF/retries.log" ]]; then
  cp "$NF/retries.log" "$HARNESS_RESULT_DIR/retries.log" 2>/dev/null || true
fi
exit "$EXIT"
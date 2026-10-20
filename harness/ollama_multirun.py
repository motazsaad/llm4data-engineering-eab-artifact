#!/usr/bin/env python3
"""Multi-run LLM prompt testing against OpenAI-compatible providers.

The default provider is OpenRouter; others are selected per model via the
`provider` field in prompts.json (see PROVIDERS below).

Run the five tasks (T1-T5) with the configured (free, open-source) models via
the OpenRouter API, for a configurable number of runs per task, and save:
  - raw model responses (JSONL),
  - a summary table (CSV) with per-run outcome and lightweight checks.

Usage:
  OPENROUTER_API_KEY=sk-or-... python3 openrouter_multirun.py \\
      --tasks T1 T3 --platforms Databricks --models GPT-4o DeepSeek-V3 \\
      --runs 3 --out results/

By default every task is run on every platform in its task definition with
every available model for --runs runs.

Checks are keyword-containing indicators, NOT the paper's execution-based
success criteria (runtime success, semantic correctness). They flag prompt
adherence only; real validation still requires executing the artifacts in
the target platform (and network access to SpaceX/httpbin), which is why the
authors may need the experimenter's help to fully validate results.
"""

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone

import requests

# Provider registry. Ollama is local (OpenAI-compatible).
PROVIDERS = {
    "ollama": {
        "base_url": "http://localhost:11434/v1",
        "models_url": "http://localhost:11434/v1/models",
        "auth_style": "none",  # no API key for local Ollama
        "key_env": None,
        "id_field": "id",
    },
}
DEFAULT_PROVIDER = "ollama"



class ProviderRateLimited(RuntimeError):
    """Upstream (free-tier provider pool) refused the request with 429.

    This is an infrastructure outage, distinct from a model producing bad code,
    so callers record it as its own outcome rather than a failed cell.
    """


class ProviderEmptyResponse(RuntimeError):
    """The provider answered 200 but returned no assistant content.

    Reasoning models routinely spend the whole `max_tokens` budget on
    `message.reasoning`, leaving `message.content` null with
    ``finish_reason == "length"``. The reply is then not an artifact at all, so
    this is a *model* outcome (it failed to emit a pipeline within budget), not
    an outage. Callers score it as a failure instead of dropping the cell.
    """


def load_config(path):
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def model_provider(model_cfg):
    """Provider key for a model entry; defaults to OpenRouter for compatibility."""
    return model_cfg.get("provider", DEFAULT_PROVIDER)


def provider_ids(model_cfg, provider):
    """Candidate model ids for this model on this provider.

    Older configs listed ids under an "openrouter" key. That is read as a
    fallback so existing model entries keep working unchanged.
    """
    ids = model_cfg.get(provider)
    if ids:
        return ids
    return model_cfg.get("openrouter", []) if provider == DEFAULT_PROVIDER else []


def resolve_model(model_cfg, available_ids, provider=DEFAULT_PROVIDER):
    """Return the first id for a model that the provider currently offers."""
    for candidate in provider_ids(model_cfg, provider):
        if candidate in available_ids:
            return candidate
    return None


def auth_headers(provider, api_key):
    """Return headers for the provider."""
    spec = PROVIDERS[provider]
    style = spec["auth_style"]
    # local ollama needs no auth
    if style == "none":
        return {}
    if not api_key:
        env = spec.get("key_env")
        if env:
            api_key = os.environ.get(env)
    if style == "raw":
        key = api_key or ""
        return {"Authorization": key}
    return {"Authorization": f"Bearer {api_key}"} if api_key else {}

def fetch_available_models(provider=DEFAULT_PROVIDER):
    """Ids the provider currently serves.

    A provider with no usable listing endpoint (Bytez) yields an empty set, and
    resolve_model then falls through to the first configured candidate: its
    request either works or fails loudly, which is preferable to pretending the
    model does not exist.
    """
    spec = PROVIDERS[provider]
    if not spec["models_url"]:
        return set()
    api_key = os.environ.get(spec["key_env"]) if spec.get("key_env") else None
    if spec["auth_style"] != "none" and not api_key:
        return set()
    resp = requests.get(spec["models_url"], headers=auth_headers(provider, api_key), timeout=30)
    resp.raise_for_status()
    field = spec["id_field"]
    return {m[field] for m in resp.json().get("data", []) if field in m}


def build_prompt(task, platform, dataset, cfg):
    lead = task["lead_ins"].get(platform)
    base = task["base_prompt"]
    dset = cfg["datasets"].get(dataset)
    if dset is None:
        raise SystemExit(f"Unknown dataset: {dataset}")
    rendered = (
        base.replace("{source}", dset.get("source", ""))
        .replace("{sink}", dset.get("sink", ""))
        .replace("{source_a}", dset.get("source_a", ""))
        .replace("{source_b}", dset.get("source_b", ""))
    )
    if lead:
        return f"{lead} {rendered}"
    return rendered


def call_chat(model_id, prompt, provider=DEFAULT_PROVIDER, api_key=None,
              temperature=0.7, max_tokens=8192, retries=6, backoff=20.0,
              ratelimit_retries=2):
    """One chat completion. Raises ProviderRateLimited on a 429 from the pool."""
    spec = PROVIDERS[provider]
    if api_key is None:
        api_key = os.environ.get(spec["key_env"]) if spec.get("key_env") else None
    if spec["auth_style"] != "none" and not api_key:
        raise RuntimeError(f"{spec['key_env']} is not set")
    payload = {
        "model": model_id,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    headers = auth_headers(provider, api_key)
    url = f"{spec['base_url']}/chat/completions"
    started = time.time()
    resp = None
    for attempt in range(retries + 1):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=600)
        except (requests.ConnectionError, requests.Timeout) as exc:
            if attempt == retries:
                raise
            wait = max(5.0, backoff / 2.0) * (attempt + 1)
            print(f"[net] {type(exc).__name__}, sleeping {wait:.0f}s "
                  f"(attempt {attempt + 1}/{retries})", flush=True)
            time.sleep(wait)
            continue
        # 429 is budget exhaustion and 5xx is transient upstream overload; both
        # are infrastructure conditions, so neither may be scored as a model
        # failure. Retrying them is what keeps a sweep from acquiring holes that
        # later have to be re-run by hand.
        if resp.status_code == 429 or resp.status_code >= 500:
            # 5xx blips clear in seconds, so they get the full retry budget. A
            # 429 from the shared free pool, by contrast, lasts hours: retrying
            # it for seven minutes just stalls the whole sweep, and the driver
            # re-attempts every gap on each round anyway. So give up early.
            limit = ratelimit_retries if resp.status_code == 429 else retries
            if attempt >= limit:
                break
            wait = (backoff if resp.status_code == 429 else max(5.0, backoff / 2.0)) * (attempt + 1)
            print(f"[{resp.status_code}] provider unavailable, sleeping {wait:.0f}s "
                  f"(attempt {attempt + 1}/{limit})", flush=True)
            time.sleep(wait)
            continue
        break
    elapsed = time.time() - started
    if resp.status_code in (402, 404) or resp.status_code == 429 \
            or resp.status_code >= 500:
        # 429/5xx are transient, and 402/404 mean the slug was withdrawn (free
        # models are removed mid-session). None of them may reach
        # raise_for_status(): that would raise a bare HTTPError, crash
        # generate_artifact.py and drop the cell from the matrix. All are
        # infrastructure, so surface them as the one exception the harness
        # records as a coverage row instead.
        note = (f"after {retries + 1} attempts"
                if resp.status_code == 429 or resp.status_code >= 500
                else "slug withdrawn")
        raise ProviderRateLimited(
            f"HTTP {resp.status_code} {note}: {resp.text[:400]}")
    resp.raise_for_status()
    body = resp.json()
    choice = body["choices"][0]
    usage = body.get("usage", {})
    content = (choice.get("message") or {}).get("content")
    if not content:
        # 200-but-empty is not an outage and must not crash the caller: it is a
        # model that answered with reasoning only (or nothing).
        raise ProviderEmptyResponse(
            f"empty content (finish_reason={choice.get('finish_reason')})")
    return {
        "content": content,
        "finish_reason": choice.get("finish_reason"),
        "prompt_tokens": usage.get("prompt_tokens"),
        "completion_tokens": usage.get("completion_tokens"),
        "elapsed_s": round(elapsed, 3),
        "provider": provider,
        "raw": {"model": body.get("model"), "id": body.get("id")},
    }


def call_openrouter(model_id, prompt, **kwargs):
    """Backwards-compatible alias for callers pinned to the OpenRouter path."""
    kwargs.setdefault("provider", DEFAULT_PROVIDER)
    kwargs.setdefault("api_key", os.environ.get(PROVIDERS[DEFAULT_PROVIDER]["key_env"]))
    return call_chat(model_id, prompt, **kwargs)


def run_checks(task_id, content, cfg):
    if task_id == "T3":
        task_id = "T3"
    kw = cfg["validation_checks"].get(task_id, [])
    lowered = content.lower()
    return {k: (k.lower() in lowered) for k in kw}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", default=os.path.join(os.path.dirname(__file__), "prompts.json"))
    ap.add_argument("--out", default="results")
    ap.add_argument("--runs", type=int, default=1)
    ap.add_argument("--dataset", default=None, help="dataset key in prompts.json")
    ap.add_argument("--tasks", nargs="*", default=None, help="subset of T1..T3")
    ap.add_argument("--platforms", nargs="*", default=None)
    ap.add_argument("--models", nargs="*", default=None, help='subset of model display names')
    ap.add_argument("--temperature", type=float, default=0.7)
    ap.add_argument("--sleep", type=float, default=1.0, help="seconds between calls")
    args = ap.parse_args()

    cfg = load_config(args.config)
    dataset = args.dataset or next(iter(cfg["datasets"]))
    os.makedirs(args.out, exist_ok=True)

    # Group the selected models by provider so each listing endpoint is queried
    # once and each key is read once.
    selected = {n: c for n, c in cfg["models"].items()
                if not args.models or n in args.models}
    by_provider = {}
    for name, mcfg in selected.items():
        by_provider.setdefault(model_provider(mcfg), []).append(name)

    available = {}
    for prov, names in by_provider.items():
        key_env = PROVIDERS[prov]["key_env"]
        if not os.environ.get(key_env):
            print(f"[skip] {', '.join(names)}: {key_env} is not set")
            continue
        try:
            available[prov] = fetch_available_models(prov)
        except Exception as exc:
            print(f"[warn] {prov} model listing failed ({exc}); trying configured ids")
            available[prov] = set()

    resolved = {}
    for prov, names in by_provider.items():
        if prov not in available:
            continue
        for name in names:
            mcfg = selected[name]
            rid = resolve_model(mcfg, available[prov], prov)
            if rid is None:
                # No listing endpoint (Bytez) or the id is not served. Fall back
                # to the first configured candidate so a working model is not
                # silently dropped; a bad id fails loudly at call time.
                cands = provider_ids(mcfg, prov)
                rid = cands[0] if cands and not available[prov] else None
                if rid is None:
                    print(f"[skip] {name}: no id available on {prov}")
                    continue
            resolved[name] = (rid, mcfg["paper_id"], prov)
        print(f"[ok]   {name}: {rid} on {prov}  (paper: {mcfg['paper_id']})")

    if not resolved:
        sys.exit("No models available")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    raw_path = os.path.join(args.out, f"raw_{dataset}_{stamp}.jsonl")
    csv_path = os.path.join(args.out, f"summary_{dataset}_{stamp}.csv")

    with open(raw_path, "w", encoding="utf-8") as raw_fh, open(csv_path, "w", newline="", encoding="utf-8") as csv_fh:
        writer = csv.writer(csv_fh)
        header = ["dataset", "model", "provider", "model_id", "openrouter_id", "paper_id",
                  "task", "platform", "run",
                  "finish_reason", "elapsed_s", "prompt_tokens", "completion_tokens"]
        writer.writerow(header)

        for task_id, task in cfg["tasks"].items():
            if args.tasks and task_id not in args.tasks:
                continue
            for platform in task["platforms"]:
                if args.platforms and platform not in args.platforms:
                    continue
                prompt = build_prompt(task, platform, dataset, cfg)
                for name, (rid, pid, prov) in resolved.items():
                    for run_idx in range(1, args.runs + 1):
                        try:
                            result = call_chat(rid, prompt, provider=prov,
                                              temperature=args.temperature)
                        except ProviderRateLimited as exc:
                            print(f"[429]  {task_id}/{platform}/{name} run {run_idx} "
                                  f"on {prov}: provider pool exhausted")
                            continue
                        except requests.RequestException as exc:
                            print(f"[err]  {task_id}/{platform}/{name} run {run_idx}: {exc}")
                            continue
                        checks = run_checks(task_id, result["content"], cfg)
                        record = {
                            "model": name, "provider": prov, "model_id": rid,
                            "openrouter_id": rid if prov == DEFAULT_PROVIDER else "",
                            "paper_id": pid,
                            "dataset": dataset, "task": task_id, "platform": platform, "run": run_idx,
                            "temperature": args.temperature,
                            **{k: v for k, v in result.items()},
                            "checks": checks, "prompt": prompt,
                        }
                        raw_fh.write(json.dumps(record, ensure_ascii=False) + "\n")
                        raw_fh.flush()
                        passed = sum(checks.values())
                        total = len(checks)
                        writer.writerow([dataset, name, prov, rid,
                                         rid if prov == DEFAULT_PROVIDER else "", pid,
                                         task_id, platform, run_idx,
                                         result["finish_reason"], result["elapsed_s"],
                                         result["prompt_tokens"], result["completion_tokens"]])
                        csv_fh.flush()
                        print(f"[done] {task_id}/{platform}/{name} run {run_idx}: checks {passed}/{total}, "
                              f"{result['completion_tokens']} tok, {result['elapsed_s']}s")
                        time.sleep(args.sleep)

    print(f"\nRaw responses : {raw_path}")
    print(f"Summary table : {csv_path}")


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""Generate an artifact for one (task, platform, dataset, model) cell via OpenRouter.

Saves:
  <out>/<dataset>-<model_slug>-<task>-<platform>.prompt.txt   (rendered prompt)
  <out>/<dataset>-<model_slug>-<task>-<platform>.<ext>        (extracted artifact)
  <out>/<dataset>-<model_slug>-<task>-<platform>.raw.json    (full API response)

For Airflow the artifact is Python source; for n8n it is a workflow JSON. Code
fences are stripped so the stored artifact is executable/importable as-is.
"""

import argparse
import json
import os
import re
import sys
import time

HERE = os.path.dirname(__file__)
ROOT = HERE
sys.path.insert(0, ROOT)

from ollama_multirun import (load_config, resolve_model, fetch_available_models,
                            call_chat, model_provider, provider_ids,
                            PROVIDERS, DEFAULT_PROVIDER, ProviderRateLimited,
                            ProviderEmptyResponse)
from render_prompt import render


def slug(s):
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def strip_fences(content):
    """Extract the actionable code/JSON from a model response.

    Prefers the single fenced block when the whole reply is one fence; otherwise
    picks the longest fenced block (models often sandwich code in prose).
    """
    fence_re = re.compile(r"```(?:[a-zA-Z0-9_+.#-]*)\s*\n(.*?)```", re.DOTALL)
    blocks = fence_re.findall(content)
    if blocks:
        return max((b.strip() for b in blocks), key=len)
    m = re.match(r"^\s*```(?:[a-zA-Z0-9_+-]*)\s*\n(.*?)\s*```\s*$", content, re.DOTALL)
    if m:
        return m.group(1).strip()
    return content.strip()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--task", required=True, choices=["T1", "T2", "T3"])
    ap.add_argument("--platform", required=True, choices=["Airflow", "n8n"])
    ap.add_argument("--dataset", default="spacex")
    ap.add_argument("--model", required=True)
    ap.add_argument("--config", default=os.path.join(ROOT, "prompts.json"))
    ap.add_argument("--fixture-map", default=os.path.join(HERE, "fixture_map.json"))
    ap.add_argument("--base-url", default=None)
    ap.add_argument("--temperature", type=float, default=0.7)
    ap.add_argument("--max-tokens", type=int, default=8192)
    ap.add_argument("--out", default="artifacts")
    ap.add_argument("--repair-prompt", default=None, help="append repair feedback to prompt")
    ap.add_argument("--use-existing", action="store_true", help="skip API call if artifact exists")
    args = ap.parse_args()

    cfg = load_config(args.config)
    # Resolved lazily: a control/self-test artifact (see selftest.sh) is keyed
    # by a placeholder name with no entry in prompts.json, and is never
    # generated, so an unknown model is only fatal when we actually call out.
    model_cfg = cfg["models"].get(args.model)
    fixture_map = json.load(open(args.fixture_map, encoding="utf-8"))
    prompt = render(cfg, args.task, args.platform, args.dataset,
                    base_url=args.base_url, fixture_map=fixture_map)
    if args.repair_prompt:
        rp = open(args.repair_prompt, "r", encoding="utf-8").read()
        prompt = prompt + "\n\n" + rp

    ms = slug(args.model)
    prefix = f"{args.dataset}-{ms}-{args.task}-{args.platform}"
    ext = "json" if args.platform == "n8n" else "py"
    os.makedirs(args.out, exist_ok=True)
    prompt_path = os.path.join(args.out, prefix + ".prompt.txt")
    artifact_path = os.path.join(args.out, prefix + "." + ext)
    raw_path = os.path.join(args.out, prefix + ".raw.json")

    with open(prompt_path, "w", encoding="utf-8") as fh:
        fh.write(prompt)
    print(f"[prompt] {prompt_path}")

    if args.use_existing and os.path.exists(artifact_path):
        print(f"[skip ] artifact exists: {artifact_path}")
        print(artifact_path)
        return 0

    if model_cfg is None:
        sys.exit(f"Unknown model: {args.model}")

    provider = model_provider(model_cfg)
    available = fetch_available_models(provider)
    model_id = resolve_model(model_cfg, available, provider)
    if model_id is None:
        # A provider with no usable listing endpoint (Bytez) cannot confirm
        # availability, so fall back to the first configured id and let the
        # request itself decide. Anything else is genuinely unavailable.
        cands = provider_ids(model_cfg, provider)
        model_id = cands[0] if cands and not available else None
    if model_id is None:
        sys.exit(f"No available {provider} model for {args.model}")

    try:
        result = call_chat(model_id, prompt, provider=provider,
                           temperature=args.temperature, max_tokens=args.max_tokens)
    except ProviderRateLimited as exc:
        # Record the outage so the matrix can distinguish provider downtime from
        # a model that produced unrunnable code. Exit code 3 = infrastructure.
        outage = {
            "outcome": "infrastructure",
            "reason": "provider_rate_limited",
            "model_display": args.model,
            "provider": provider,
            "model_id": model_id,
            "provider_model_id": model_id,
            "task": args.task,
            "platform": args.platform,
            "dataset": args.dataset,
            "detail": str(exc)[:500],
        }
        with open(raw_path, "w", encoding="utf-8") as fh:
            json.dump(outage, fh, indent=2)
        print(f"[infrastructure] provider rate-limited after retries: {model_id}")
        print(f"[raw]            {raw_path}")
        return 3
    except ProviderEmptyResponse as exc:
        # 200 but no assistant content: the model answered with reasoning only.
        # That is a model outcome, so the cell is scored as a failure rather
        # than dropped (exit code 4 = no artifact produced).
        empty = {
            "outcome": "no_content",
            "reason": "no_artifact",
            "model_display": args.model,
            "provider": provider,
            "model_id": model_id,
            "task": args.task,
            "platform": args.platform,
            "dataset": args.dataset,
            "detail": str(exc)[:500],
        }
        with open(raw_path, "w", encoding="utf-8") as fh:
            json.dump(empty, fh, indent=2)
        print(f"[no-content] model returned no artifact: {model_id}")
        print(f"[raw]            {raw_path}")
        return 4
    content = result["content"]
    artifact = strip_fences(content)

    with open(artifact_path, "w", encoding="utf-8") as fh:
        fh.write(artifact + "\n")
    with open(raw_path, "w", encoding="utf-8") as fh:
        result["model_display"] = args.model
        result["provider"] = provider
        result["model_id"] = model_id
        result["provider_model_id"] = model_id
        result["task"] = args.task
        result["platform"] = args.platform
        result["dataset"] = args.dataset
        result["prompt"] = prompt
        json.dump(result, fh, indent=2)

    print(f"[artifact] {artifact_path}")
    print(f"[raw]      {raw_path}")
    print(f"[model]    {provider}:{model_id}  finish={result['finish_reason']} "
          f"tok={result['completion_tokens']} {result['elapsed_s']}s")
    print(artifact_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
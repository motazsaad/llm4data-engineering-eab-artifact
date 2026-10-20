#!/usr/bin/env python3
"""Render a prompt for one (task, platform, dataset) cell, hermetic or live.

Hermetic mode rewrites the dataset URLs in prompts.json so that the generated
artifact is forced to talk to the local recorder_fixtures source and the
recorder sink. This keeps validation deterministic and offline.
"""

import argparse
import json
import os
import sys

HERE = os.path.dirname(__file__)
ROOT = HERE
sys.path.insert(0, ROOT)

from ollama_multirun import load_config, build_prompt


def hermetic_urls(cfg, dataset, base_url, fixture_map):
    dset = cfg["datasets"][dataset]
    fm = fixture_map.get(dataset, {})
    urls = {
        "source": dset.get("source", ""),
        "source_a": dset.get("source_a", ""),
        "source_b": dset.get("source_b", ""),
        "sink": dset.get("sink", ""),
    }
    for key in ("source", "source_a", "source_b"):
        rel = fm.get(key)
        if rel:
            urls[key] = f"{base_url}/fixtures/{rel}"
    urls["sink"] = f"{base_url}/sink"
    return urls


def render(cfg, task_id, platform, dataset, base_url=None, fixture_map=None):
    task = cfg["tasks"][task_id]
    dset = cfg["datasets"][dataset]
    if base_url is not None:
        urls = hermetic_urls(cfg, dataset, base_url, fixture_map or {})
    else:
        urls = {k: dset.get(k, "") for k in ("source", "source_a", "source_b", "sink")}
    lead = task["lead_ins"].get(platform, "")
    base = task["base_prompt"]
    rendered = (
        base.replace("{source}", urls["source"])
        .replace("{sink}", urls["sink"])
        .replace("{source_a}", urls["source_a"])
        .replace("{source_b}", urls["source_b"])
    )
    if lead:
        rendered = f"{lead} {rendered}"
    return rendered


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--task", required=True, choices=["T1", "T2", "T3"])
    ap.add_argument("--platform", required=True, choices=["Airflow", "n8n"])
    ap.add_argument("--dataset", default="spacex")
    ap.add_argument("--config", default=os.path.join(ROOT, "prompts.json"))
    ap.add_argument("--fixture-map", default=os.path.join(HERE, "fixture_map.json"))
    ap.add_argument("--base-url", default=None,
                    help="hermetic recorder base URL, e.g. http://recorder:8055 (host in containers)")
    args = ap.parse_args()

    cfg = load_config(args.config)
    fixture_map = json.load(open(args.fixture_map, encoding="utf-8"))
    prompt = render(cfg, args.task, args.platform, args.dataset,
                    base_url=args.base_url, fixture_map=fixture_map)
    print(prompt)


if __name__ == "__main__":
    main()
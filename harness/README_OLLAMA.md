# Local Ollama harness (self-contained)

Execution-based evaluation of LLM-authored data pipelines against a **local
Ollama** server instead of OpenRouter/NVIDIA. This directory is a complete copy
of the Airflow + n8n harness
(`../openrouter_multirun/harness/`) with the provider and model list swapped.

- Provider: `http://localhost:11434/v1` (OpenAI-compatible, **no API key**).
- Models configured (`prompts.json`): `gemma4:12b`, `qwen3.8:27b`, `cogito:32b`.
- Datasets: `spacex`, `github`. Tasks: `T1` (ingest), `T2` (ETL), `T3` (join).
- Platforms: `Airflow`, `n8n`.

## Prerequisites
- Ollama running: `ollama serve` (or the systemd service).
- Models pulled: `ollama pull gemma4:12b qwen3.8:27b cogito:32b`.
- Docker + Docker Compose.

## Run
```bash
cd llm4data_icde2027_v3/local_ollama

# 1. Validate the instrument before blaming a model (no model calls).
for t in T1 T2 T3; do for d in spacex github; do
  bash selftest.sh --task "$t" --dataset "$d"
done; done

# 2. Sweep the matrix (all configured models x T1-T3 x both platforms).
bash run_matrix.sh --dataset spacex --runs 3 --hermetic
bash run_matrix.sh --dataset github --runs 3 --hermetic

# filters: --models gemma4 --tasks T1
# 3. Reduce and produce paper tables.
python3 aggregate.py    --matrix-dir out --out out/summary
python3 paper_tables.py --matrix-dir out --out out/paper
```

Run the recorder once (`docker compose up -d recorder`) or let `run_one.sh`
start it. Results land under `out/` (per-cell dirs + `matrix_<dataset>_<task>.csv`).

## Notes / caveats
- Cell paths sanitize `:` and spaces in model display names: Ollama tags
  (`gemma4:12b`) would otherwise break Docker's `-v host:container:ro` mount.
- `__reference__` rows are control cells; exclude them from model tables
  (`paper_tables.py` does this automatically).
- n8n cells may need transport retries (`N8N_EXEC_ATTEMPTS`); report the retry
  counts alongside n8n numbers (`paper_tables.py` emits a retry / no-retry
  split).
- The n8n container writes as uid 1000, so `run_n8n.sh` chmods the run dir to be
  world-writable regardless of the host user.

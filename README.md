# LLM4Data EAB Artifact

Reproducibility artifact for the ICDE 2027 Experiment, Analysis, and Benchmark
paper, *An Automated Execution-Based Benchmark for LLM Code Generation in
Data-Engineering Environments*.

It contains the complete execution harness, recorded data fixtures, prompts,
container definitions, reference controls, and the result summary used in the
paper. The harness evaluates generated Airflow and n8n artifacts against a
local recorder and sink. Its runner-adapter boundary is designed to accommodate
another containerized open-source data-engineering tool.

## Contents

- `harness/`: execution code, Docker Compose configuration, Airflow/n8n runners,
  recorder, scoring code, prompt templates, and repair protocol.
- `harness/fixtures/`: hermetic recorded fixtures for the SpaceX and GitHub
  benchmark datasets.
- `harness/selftest/`: hand-written control artifacts; all must pass before a
  model is measured.
- `RESULTS.md`: the released experiment summary and protocol.

## Quick start

Prerequisites: Docker with Compose, Python 3, and an Ollama server running on
the host. Pull the models listed in `harness/prompts.json`, then run:

```bash
cd harness

# Validate the instrument without calling a model.
for task in T1 T2 T3; do
  for dataset in spacex github; do
    bash selftest.sh --task "$task" --dataset "$dataset"
  done
done

# Run the hermetic baseline matrix and render summary tables.
bash run_matrix.sh --dataset spacex --runs 5 --hermetic
bash run_matrix.sh --dataset github --runs 5 --hermetic
python3 aggregate.py --matrix-dir out --out out/summary
python3 paper_tables.py --matrix-dir out --out out/paper
```

See [`harness/README_OLLAMA.md`](harness/README_OLLAMA.md) for the full
protocol, repair experiment, and operational notes.

## Data and results

The six fixture files are recorded, static API responses replayed by the local
recorder; evaluation does not depend on public endpoints. The results released
in `RESULTS.md` are the study summary. New runs write their generated artifacts,
per-cell score records, and matrices below `harness/out/`, which is intentionally
ignored because it can contain many generated model artifacts.

## Citation

If you use this artifact, cite the accompanying paper and this repository using
the metadata in [`CITATION.cff`](CITATION.cff). A tagged release should be used
for archival citation.

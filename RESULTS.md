# Ollama Experiment — Results Summary

**Harness:** execution-based evaluation of LLM-authored data pipelines (Airflow + n8n),
`llm4data_icde2027_v3/local_ollama/`.
**Provider:** local Ollama (`http://localhost:11434/v1`, no API key).
**Date:** 2026-10-16. **Protocol:** hermetic (fixtures replayed by local recorder).

---

## 1. Scope

| axis | values |
|---|---|
| Models | `gemma4:12b`, `qwen3.8:27b`, `cogito:32b` (headline, **k = 5**), `qwen3-coder:30b` (ceiling, k = 3) |
| Datasets | `spacex`, `github` |
| Tasks | `T1` (ingest 1→sink), `T2` (ETL src→transform→sink), `T3` (2 sources joined on shared key→sink) |
| Platforms | `Airflow`, `n8n` |
| Replicates | k = 5 for the 3 headline models, k = 3 for the ceiling model |
| **Baseline samples** | **216** (3×3×2×2×5 = 180 headline + 3×2×2×3 = 36 ceiling) |
| Controls | 12 (1 per dataset×task×platform), hand-written reference artifacts |

The headline three were later raised from k = 3 to k = 5 (the k = 3 pilot is snapshotted in
`out/k3_snapshot/`); `qwen3-coder:30b` was added as a stronger local "ceiling" model to test
task *attainability*.

---

## 2. Instrument validation (controls — 12/12 PASS)

Before any model was run, every control artifact scored PASS on both platforms:

```
github  T1/T2/T3 × Airflow/n8n  = 6/6
spacex  T1/T2/T3 × Airflow/n8n  = 6/6
```

Per the experiment rule: *if a control does not score PASS, the defect is in the harness,
not the model.* All controls pass, so model failures are genuine.

Outputs: `out/paper/paper_controls_table.tex`, `out/matrix_*.csv` (`__reference__` rows).

---

## 3. Baseline results (single-shot generation)

**Overall: 20 / 216 = 0.093.** Infra failures: 0. Timeouts: 1.

By task (pass@1):

| task | overall | Airflow | n8n |
|---|---|---|---|
| **T1** ingest | 19/72 = **0.264** | 11/36 = 0.306 | 8/36 = 0.222 |
| **T2** ETL | 1/72 = **0.014** | 1/36 = 0.028 | 0/36 = 0.000 |
| **T3** join | 0/72 = **0.000** | 0/36 = 0.000 | 0/36 = 0.000 |

By model (pass@1): `qwen3.8:27b` 10/60 = **0.167**, `qwen3-coder:30b` 5/36 = 0.139,
`gemma4:12b` 3/60 = 0.050, `cogito:32b` 2/60 = 0.033.
By dataset: github 11/108 = 0.102, spacex 9/108 = 0.083.

Per-cell pass rates (k = 5 headline, k = 3 ceiling):

| dataset | task | platform | cogito:32b | gemma4:12b | qwen3.8:27b | qwen3-coder:30b |
|---|---|---|---|---|---|---|
| github | T1 | Airflow | 0.40 | 0.00 | **0.60** | **0.67** |
| github | T1 | n8n | 0.00 | 0.20 | 0.40 | 0.00 |
| github | T2 | Airflow | 0.00 | 0.00 | 0.00 | **0.33** |
| github | T2 | n8n | 0.00 | 0.00 | 0.00 | 0.00 |
| github | T3 | Airflow / n8n | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 |
| spacex | T1 | Airflow | 0.00 | 0.00 | 0.20 | **0.67** |
| spacex | T1 | n8n | 0.00 | 0.40 | **0.80** | 0.00 |
| spacex | T2 | Airflow / n8n | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 |
| spacex | T3 | Airflow / n8n | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 | 0.00 / 0.00 |

**Attainability (ceiling model).** `qwen3-coder:30b` is the only model that passes **T2**
(github Airflow, 1/3) — so T2 is *not* an impossible task. It also matches the best T1
result (0.67) yet fails **all** n8n cells, i.e. competent code is not sufficient for the
n8n workflow-JSON surface. **T3 (relational join) is 0/72 for every model.**

Failure causes are genuine model errors (verified by inspecting generated artifacts,
tracebacks, and `score.json` check breakdowns), e.g.:

- `with DAG(...) as:` — missing context variable (`SyntaxError`)
- `DAG(..., catch_up=...)` — invalid kwarg (Airflow uses `catchup`)
- malformed workflow JSON emitted by the model (`JSONDecodeError`)
- n8n HTTP node body not valid JSON
- join produced 100 rows instead of the expected 3 (wrong/duplicated join semantics)

Outputs: `out/summary/summary.{csv,md}`, `out/paper/paper_main_table.tex`, `out/paper/paper_cells.csv`.
The earlier k = 3 pilot numbers (8/108 = 0.074, T1 = 0.222) are preserved in
`out/k3_snapshot/`; the k = 3 replicates were noisy and are superseded by k = 5.

---

## 4. Repair experiment (execution feedback, R ≤ 2)

### Protocol

1. Seed = a baseline cell that **FAILED** single-shot.
2. Round *r*: prompt = *previous artifact + concise failure feedback* (essential error
   lines with deprecation noise stripped, failing checks, actual-vs-expected) +
   instruction to return only the corrected artifact.
3. Re-generate → re-execute on the same platform → re-score with the same `assert`.
4. Stop on PASS or after `R=2`.

All repair artifacts write to `out/repair/` — **baseline is never modified** (verified:
`out/matrix_*.csv` unchanged, 0 matrix↔cell mismatches).

**Tooling added** (`local_ollama/`): `repair_feedback.py`, `repair_one.sh`,
`run_matrix_repair.sh`, `aggregate_repair.py`, `generate_artifact.py --repair-prompt`,
`run_one.sh --repair-prompt` + `CELLS_ROOT` / `MATRIX_DIR` overrides.

**Runs attempted:** 100 failed baseline samples (R≤2). **Recovered: 21.**

### Results (k = 3 pilot, 108 samples — superseded, kept for reference)

**Overall: 8/108 = 0.074 → 29/108 = 0.269 (≈3.6×), 21 samples recovered.**

| slice | pass@1 | pass@R=2 |
|---|---|---|
| **Overall** | **0.074** (8) | **0.269** (29) |
| T1 / T2 / T3 | .222 / .000 / .000 | .556 / .222 / .028 |
| Airflow / n8n | .056 / .093 | .352 / .185 |
| cogito:32b / gemma4:12b / qwen3.8:27b | .028 / .056 / .139 | .111 / .194 / **.500** |
| github / spacex | .093 / .056 | .370 / .167 |

### Results (k = 5 headline + ceiling, 216 samples — current)

**Runs attempted:** 195 of the 196 baseline failures (R≤2; one failed run has no
repair record). **Recovered: 36.**
**Overall: 20/216 = 0.093 → 56/216 = 0.259 (≈2.8×).**

| slice | pass@1 | pass@R=2 |
|---|---|---|
| **Overall** | **0.093** (20) | **0.259** (56) |
| T1 / T2 / T3 | .264 / .014 / .000 | **.542 / .181 / .056** |
| Airflow / n8n | .102 / .083 | **.370 / .148** |
| cogito:32b / gemma4:12b / qwen3.8:27b / qwen3-coder:30b | .033 / .050 / .167 / .139 | .133 / .200 / **.517** / .139 |
| github / spacex | .102 / .083 | .343 / .176 |

**Interpretation:** repair recovers *surface* errors (syntax, invalid API kwargs,
malformed JSON), far more so on Airflow (`.102 → .370`, 3.6×) than on n8n (`.083 → .148`,
1.8×). It now reaches **all three tasks**: T2 rises to `.181` and T3 to `.056` (4/72) —
so T3 is repairable, not impossible. `qwen3.8:27b` gains the most (`.167 → .517`);
`qwen3-coder:30b` gains **nothing** (`.139 → .139`), i.e. its failures are semantic,
not surface — competent code does not help when the join/model transform is wrong.
The platform/task gap is therefore not a harness artifact — it is a capability boundary
that execution feedback only partly closes.

Outputs: `out/repair/repair_summary.{csv,md}`, `out/repair/cells/`, `out/repair/manifests/`,
`out/repair/matrix_*.csv`, `out/paper/paper_repair_table.tex`.

---

## 5. Key numbers for the paper

- Controls: **12/12 PASS** (instrument validated).
- Baseline (single-shot, k=5 headline + k=3 ceiling): **20/216 = 9.3%**;
  **T2 = 1.4%, T3 = 0.0%** across all models.
- Attainability: `qwen3-coder:30b` passes **T2** (github Airflow, 1/3) → T2 is
  model-attainable; **T3 is 0/72 for every model**.
- Execution-feedback repair (R≤2): **56/216 = 25.9%**, +36 samples, ≈2.8×.
  After repair: T1 = 54.2%, T2 = 18.1%, **T3 = 5.6%** (repair reaches all tasks).
- Best single model: `qwen3.8:27b` → 10/60 (0.167) single-shot, 31/60 (0.517) after repair.
- Platform: Airflow repairs far better than n8n (3.6× vs 1.8×).
- No infrastructure failures (1 timeout total; all cells scored by the real platform run).

---

## 6. Caveats / protocol notes to disclose

- **k = 5** for the three headline models, **k = 3** for `qwen3-coder:30b` (ceiling probe).
  The earlier k = 3 pilot (`out/k3_snapshot/`) was noisy — e.g. T1 overall moved
  0.222 → 0.264 and individual cells shifted by ≥1 replicate; k = 5 is the reported set.
- Controls are **k = 1** (hand-written, deterministic).
- `n8n (retry)` vs `n8n (no retry)` rows are identical for every model cell: **no
  transport retry occurred on any model run**. Across all 84 baseline n8n runs the
  engine retried exactly once, and that run was the `__reference__` control at
  `github/T3/n8n/run1`. Repair (37 runs) and the size ladder are likewise all zero.
  Note the accounting bug this replaces: `run_one.sh` did not export the cell dir
  and `run_n8n.sh` never copied the container's `retries.log`, so
  `paper_tables.retries_for()` found no per-cell record and scored every n8n pass as
  a non-first-attempt pass. That manufactured a platform-retry effect which the raw
  `run.log` files do not support. Fixed; `retries_for()` now falls back to
  `run.log` and returns `None` (unknown) rather than `False`.
- An early buggy prototype of `repair_one.sh` briefly re-ran 2 baseline cells before the
  `CELLS_ROOT`/`MATRIX_DIR` override existed. Fixed: matrix deduped to unique keys
  (first occurrence = true baseline), both cells restored, reconciliation → **0 mismatches**.
- `run_matrix.sh --run-start N` extends replicates (used to add runs 4–5 without redoing
  1–3); `run_matrix_repair.sh` now skips already-repaired runs unless `--force`.
- Repair uses `R = 2`, not 3 (chosen scope; see `paper_repair_table.tex` for pass@1→pass@R).

---

## 7. How to run

```bash
cd llm4data_icde2027_v3/local_ollama

# instrument validation
bash selftest.sh --task T1 --dataset spacex      # repeat for T2,T3 × spacex,github

# baseline sweep (k=5 headline three, both datasets)
bash run_matrix.sh --models "gemma4,qwen3.8,cogito" --dataset spacex --runs 5 --hermetic
bash run_matrix.sh --models "gemma4,qwen3.8,cogito" --dataset github --runs 5 --hermetic

# ceiling model
bash run_matrix.sh --models qwen3-coder --dataset spacex --runs 3 --hermetic
bash run_matrix.sh --models qwen3-coder --dataset github --runs 3 --hermetic

python3 aggregate.py    --matrix-dir out --out out/summary
python3 paper_tables.py --matrix-dir out --out out/paper

# extend an existing k=3 sweep to k=5 without redoing runs 1-3
bash run_matrix.sh --models "gemma4,qwen3.8,cogito" --dataset spacex --runs 5 --run-start 4 --hermetic

# repair ablation (failed baseline runs; skips already-repaired unless --force)
bash run_matrix_repair.sh --rmax 2
python3 aggregate_repair.py

# end-to-end driver for the k=5 + ceiling + repair pass
bash run_paper_extra.sh
```

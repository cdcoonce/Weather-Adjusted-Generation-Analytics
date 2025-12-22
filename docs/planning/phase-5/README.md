# Phase 5 — Integration Tests (dbt, Dagster, E2E) Roadmap

Phase 5 adds *integration* coverage across the main layers of the repo:

- DuckDB + dlt ingestion wiring at a small scale
- dbt transformations against an isolated DuckDB database
- Dagster asset execution in-process (no UI)

## Goal

Increase confidence that the system works end-to-end on a *tiny* dataset, while keeping tests deterministic and reasonably fast.

## Constraints

- Integration tests may touch filesystem + DuckDB + subprocesses, but must be deterministic.
- Prefer temp directories (`tmp_path`) and temp DuckDB files; never write to repo `data/` during tests.
- Avoid relying on machine-specific absolute paths in dbt profiles.

## Documents (recommended order)

1. `01-scope-and-acceptance.md`
2. `02-dbt-integration.md`
3. `03-dagster-assets.md`
4. `04-e2e-smoke.md`
5. `05-execution-and-selection.md`

## Definition of done

- `uv run pytest -m integration` passes locally.
- A minimal dbt run executes against an isolated DuckDB database.
- A Dagster asset materialization runs in-process and returns expected outputs.
- An end-to-end smoke test covers: sample parquet → ingestion → dbt select → query mart.

## Status

Completed and verified on 2025-12-20.

## Verification commands

```bash
uv run pytest -m integration
uv run pytest
```

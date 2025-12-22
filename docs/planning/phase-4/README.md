# Phase 4 — Loader Unit Tests (I/O Heavy) Roadmap

Phase 4 focuses on tests for `weather_adjusted_generation_analytics/loaders/` modules that interact with filesystems and dlt pipeline objects.

## Goal
Add unit tests that validate loader behavior **without** running full ingestion into DuckDB.

Target modules:
- `weather_adjusted_generation_analytics/loaders/weather_loader.py`
- `weather_adjusted_generation_analytics/loaders/generation_loader.py`
- `weather_adjusted_generation_analytics/loaders/dlt_pipeline.py`

## Constraints
- Do not run real `dlt.pipeline(...).run(...)` in unit tests.
- Prefer temp parquet files (`tmp_path`) and/or committed sample parquet (`tests/data/`).
- Use `pytest-mock` / `monkeypatch` for:
  - `config.*_raw_path` (avoid reading repo `data/`)
  - `dlt.pipeline` and its `.run()` method

## Documents (recommended order)
1. `01-scope-and-acceptance.md`
2. `02-weather-and-generation-loaders.md`
3. `03-dlt-pipeline-tests.md`
4. `04-mocking-strategy.md`
5. `05-execution-and-selection.md`

## Definition of done
- Loader unit tests run under `-m unit` and complete quickly.
- Tests validate:
  - record-yielding logic from parquet
  - pipeline creation configuration (names, destination credentials, dataset name)
  - orchestration sequencing in `run_full_ingestion()` via mocks

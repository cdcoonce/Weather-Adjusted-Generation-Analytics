# Phase 4.1 — Scope & Acceptance Criteria

## Objective
Define what we will test in loader modules and where we draw the unit/integration boundary.

## In scope (unit tests)

### `weather_loader.py` / `generation_loader.py`
- When given explicit `file_paths`, the `@dlt.resource` generator:
  - reads parquet with Polars
  - yields dictionaries
  - logs counts
- When `file_paths=None`, it discovers files under `config.<type>_raw_path`.
  - In tests, patch this to point to a temp dir (avoid repo `data/`).

### Pipeline factories
- `get_weather_pipeline()` / `get_generation_pipeline()`
  - validates `pipeline_name` suffixing
  - validates destination configuration uses `config.duckdb_path`
  - validates `dataset_name=config.dlt_schema`

### Orchestration (`dlt_pipeline.py`)
- `run_full_ingestion()`
  - ensures `config.ensure_directories()` is called
  - calls `run_weather_ingestion()` then `run_generation_ingestion()`
  - propagates exceptions

- `run_combined_pipeline()`
  - constructs a `dlt.pipeline(...)` with expected args
  - calls `.run([...])` with the two resources
  - logs failures when `load_info.has_failed_jobs` is true

## Out of scope (reserve for integration)
- Actually loading data into DuckDB via dlt
- Running `verify_ingestion()` against a real db file
- Running dbt compiled models
- Running Dagster assets

## Acceptance criteria
- New tests live under `tests/unit/` and are marked `@pytest.mark.unit`.
- Any test that touches filesystem/parquet is also marked `@pytest.mark.io`.
- Tests use Phase 2 fixtures (parquet writers, sample data) and/or `tmp_path`.
- Tests do not require network, `.env`, or a pre-existing `data/` directory.

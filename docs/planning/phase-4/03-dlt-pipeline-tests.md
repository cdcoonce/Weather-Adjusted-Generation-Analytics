# Phase 4.3 — `dlt_pipeline.py` Tests

## Objective
Validate orchestration logic and dlt pipeline wiring in `weather_adjusted_generation_analytics/loaders/dlt_pipeline.py` without performing real ingestion.

## Where tests should live
- `tests/unit/test_dlt_pipeline.py`

## What to mock
- `src.loaders.dlt_pipeline.config.ensure_directories`
- `src.loaders.dlt_pipeline.run_weather_ingestion`
- `src.loaders.dlt_pipeline.run_generation_ingestion`
- `src.loaders.dlt_pipeline.dlt.pipeline`

## Test plan

### `run_full_ingestion()` sequencing
- Patch `ensure_directories`, `run_weather_ingestion`, `run_generation_ingestion`.
- Call `run_full_ingestion()`.
- Assert:
  - `ensure_directories` called once
  - weather ingestion called before generation ingestion
  - both are called with correct args when specific file lists are passed

### `run_full_ingestion()` exception propagation
- Patch `run_weather_ingestion` to raise.
- Assert `run_full_ingestion()` raises.
- Assert `run_generation_ingestion` was not called.

### `run_combined_pipeline()` wiring
- Mock `dlt.pipeline(...)` to return a fake pipeline object whose `.run(...)` returns a fake `load_info`.
- Assert `dlt.pipeline` called with:
  - `pipeline_name=config.dlt_pipeline_name`
  - `destination=dlt.destinations.duckdb(credentials=str(config.duckdb_path))` (you can assert that credentials equals the expected DB path string rather than deep-equality on destination objects)
  - `dataset_name=config.dlt_schema`

- Assert `.run(...)` called with a list containing both resources.

### Failure logging branch
- Provide a fake `load_info` where `has_failed_jobs=True` and includes `load_packages` with failed job entries.
- Assert the function logs error messages (use `caplog`).

## Notes
- Keep these tests `@pytest.mark.unit`. They should not touch filesystem.
- Avoid asserting exact log text; assert key phrases or log levels.

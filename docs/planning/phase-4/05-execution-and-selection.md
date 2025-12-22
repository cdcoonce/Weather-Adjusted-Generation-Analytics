# Phase 4.5 — Execution & Selection

## Objective
Define how to run loader tests during Phase 4.

## Commands

### Run all unit tests

```bash
uv run pytest -m unit
```

### Run only loader-focused unit tests

```bash
uv run pytest -m unit tests/unit/test_weather_loader.py
uv run pytest -m unit tests/unit/test_generation_loader.py
uv run pytest -m unit tests/unit/test_dlt_pipeline.py
```

### Run unit tests excluding parquet I/O

```bash
uv run pytest -m "unit and not io"
```

## Marker usage
- All loader tests: `@pytest.mark.unit`
- Tests that read/write parquet: also `@pytest.mark.io`
- Tests that use DuckDB: also `@pytest.mark.duckdb` (not expected in Phase 4 unit tests; mostly Phase 2/Phase 5+)

## Acceptance criteria
- Loader test subset runs quickly (< ~5 seconds).
- `-m "unit and not io"` remains viable for fast iterations.

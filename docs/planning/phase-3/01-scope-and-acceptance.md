# Phase 3.1 — Scope & Acceptance Criteria

## Objective
Define what qualifies as “unit tests” for this repo and what we consider sufficient coverage for the first pass.

## In scope

### `src/utils/polars_utils.py`
- Transform correctness (columns created, values expected for small deterministic inputs)
- Edge cases (empty inputs where reasonable, partitioning behavior, invalid stats)
- Non-mutation expectations (functions should not unexpectedly mutate inputs)

### `src/utils/logging_utils.py`
- `JSONFormatter.format()` emits expected keys and merges `extra_fields`
- `get_logger()` returns a logger with expected handler/formatter settings
- `log_execution_time()` logs success/failure paths deterministically using `caplog`

### `src/config/settings.py`
- Defaults and types
- Derived paths (`weather_raw_path`, `generation_raw_path`)
- `ensure_directories()` creates expected directories
- Behavior with explicit overrides (construct `Config(...)` directly)

## Out of scope
- Running dlt ingestion end-to-end
- Running dbt CLI
- Running Dagster assets/jobs

## Acceptance criteria
- Each target module has a dedicated unit test file under `tests/unit/`.
- Tests have explicit markers:
  - All are `@pytest.mark.unit`
  - Add `@pytest.mark.io` where filesystem writes happen
- No test reads/writes outside `tmp_path` and `tests/data/`.
- Tests run in < ~5 seconds total.

## Suggested minimum counts
Not a hard rule, but a good baseline:
- `polars_utils`: ~8–15 tests
- `logging_utils`: ~5–10 tests
- `settings`: ~5–10 tests

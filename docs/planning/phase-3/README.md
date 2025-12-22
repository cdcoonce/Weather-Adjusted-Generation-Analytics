# Phase 3 — Unit Tests (Core Modules) Roadmap

Phase 3 uses the Phase 1 test scaffolding and Phase 2 fixtures to write **unit tests** for the highest-value pure logic and configuration behaviors.

## Goal
Add a meaningful unit-test baseline for:
- `weather_adjusted_generation_analytics/utils/polars_utils.py`
- `weather_adjusted_generation_analytics/utils/logging_utils.py`
- `weather_adjusted_generation_analytics/config/settings.py`

## What Phase 3 should produce
- New unit test modules under `tests/unit/`:
  - `test_polars_utils.py`
  - `test_logging_utils.py`
  - `test_settings.py`
- Tests that are deterministic, fast, and do not depend on real repo `data/`.

## Constraints
- Unit tests must not run dlt pipelines, dbt CLI, or Dagster jobs.
- Use `tmp_path` and `temp_config` fixtures to avoid side effects.

## Documents (recommended order)
1. `01-scope-and-acceptance.md`
2. `02-polars-utils-tests.md`
3. `03-logging-utils-tests.md`
4. `04-settings-tests.md`
5. `05-execution-and-selection.md`

## Definition of done
- `uv run pytest -m unit` passes consistently.
- Phase 3 adds coverage to `weather_adjusted_generation_analytics/utils/*` and `weather_adjusted_generation_analytics/config/*`.
- New tests are stable across platforms and do not require `.env`.

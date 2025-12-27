# TODO.md

This file tracks the repo’s current work.

## Rules

- Keep items small, actionable, and testable.
- Prefer checklists.
- If a task changes behavior or architecture, add/consult `DECISIONS.md`.

## Now

- [ ] CI improvements:
  - [ ] Add caching for uv/pip and dbt packages.

- [ ] Increase unit coverage for `weather_adjusted_generation_analytics/mock_data/`:
  - [x] Add tests for `generate_weather_data()` output shape and value bounds.
  - [ ] Add tests for `generate_generation_data()` determinism on a small date range.

- [ ] Remove Ruff deprecation warnings:
  - [ ] Migrate Ruff config in `pyproject.toml` to `tool.ruff.lint`.

## Done (recent)

- [x] Add focused unit tests for `weather_adjusted_generation_analytics/mock_data/`:
  - [x] `wind_power_curve()` edge points (cut-in/rated/cut-out).
  - [x] `solar_power_output()` scaling + clipping.

- [x] Increase unit test coverage for `weather_adjusted_generation_analytics/loaders/dlt_pipeline.py`:
  - [x] Unit-test `run_full_ingestion()` call order via mocking.
  - [x] Unit-test `run_combined_pipeline()` success and failure cases.

- [x] Decide and document the Python version policy (see `DECISIONS.md` D-0004).
- [x] Add `ARCHITECTURE.md` describing:
  - Boundaries (Dagster orchestration vs business logic)
  - Data flow (mock → parquet → dlt → duckdb → dbt → marts)
  - Where schemas live and how to query them

- [x] Add GitHub Actions CI:
  - Ruff lint (focused rules) on `weather_adjusted_generation_analytics/` + `dags/`
  - Pytest unit + integration suites

- [x] Migrate imports to a canonical package:
  - `weather_adjusted_generation_analytics/` is the canonical Python package.
  - Removed the legacy `src/` tree (no compatibility layer).
  - Dagster code does not mutate `sys.path`.

## Next

- [ ] Increase coverage for `weather_adjusted_generation_analytics/loaders/dlt_pipeline.py`:
  - [ ] Decide whether `verify_ingestion()` should be schema-agnostic (likely yes).

- [ ] CI improvements:
  - [ ] (Optional) Add a Python version matrix if we want earlier signal on 3.13.
  - [ ] Consider splitting unit vs integration into parallel jobs (already separated).
  - [ ] Enable branch protection requiring CI checks (GitHub settings).

## Later

- [ ] Add coverage threshold enforcement once coverage stabilizes (start with a realistic target).
- [ ] Add a lightweight "smoke" Makefile or task runner section for common commands.
- [ ] Add a CONTRIBUTING.md describing style, testing commands, and marker usage.

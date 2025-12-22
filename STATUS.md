# STATUS.md

Current project status for humans and future agents.

## Current status (2025-12-21)

- ✅ Repo is under git and pushed to GitHub.
- ✅ Python policy standardized: **Python 3.12+** (see `DECISIONS.md` D-0004).
- ✅ Test suites exist and are green locally:
  - `uv run python -m pytest -m unit`
  - `uv run python -m pytest -m integration`
  - `uv run python -m pytest`
- ✅ CI is enabled via GitHub Actions:
  - Lint: Ruff (focused rules) on `weather_adjusted_generation_analytics/` and `dags/`
  - Tests: unit → integration

## CI details

- Workflow: `.github/workflows/ci.yml`
- Jobs:
  - `lint`: `uv run ruff check weather_adjusted_generation_analytics dags --select F,E9,I`
  - `unit`: `uv run python -m pytest -m unit -q`
  - `integration`: `uv run python -m pytest -m integration -q`

## Open items (highest priority)

- Add CI caching for uv/pip and dbt packages (speed + stability).
- Add unit tests for `weather_adjusted_generation_analytics/loaders/dlt_pipeline.py` orchestration paths.

## Notes / risks

- Ruff is intentionally scoped to `weather_adjusted_generation_analytics/` and `dags/` with a minimal ruleset to avoid blocking on repo-wide formatting/lint cleanup.
- Ruff config currently emits deprecation warnings about moving settings under `tool.ruff.lint`.

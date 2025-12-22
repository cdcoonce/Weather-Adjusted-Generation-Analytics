# Phased Pytest Plan

This document outlines a phased approach to introduce a robust pytest test suite for this repository. Each phase contains goals, deliverables, and suggested artifacts to produce.

## Phase 0 — Repo review & baseline
- Goal: Understand code structure, runtime/deps, and where tests will attach.
- Deliverables: a concrete baseline snapshot + a prioritized test target inventory.
- Artifacts: this planning doc and the Phase 1 roadmap docs in `docs/planning/phase-1/`.

### Baseline snapshot (as-is)
- **Python**: `>=3.12` (from `pyproject.toml`).
- **Package manager**: `uv` (see `GETTING_STARTED.md` and `setup.sh`).
- **Project metadata/build**: Hatchling (`build-backend = "hatchling.build"`).
- **Existing pytest config**: already present in `[tool.pytest.ini_options]` in `pyproject.toml`:
  - `testpaths = ["tests"]`
  - `python_files = ["test_*.py"]`
  - `addopts = "-v --strict-markers"`
- **Existing dev deps**: `pytest` and `pytest-mock` already included under `[project.optional-dependencies].dev`.
- **Missing pieces for a “real” test suite**:
  - No `tests/` directory exists yet.
  - No coverage tooling (`pytest-cov`) is currently listed in dev deps.

### What to test first (highest ROI)
These are good early candidates because they’re deterministic and don’t require standing up Dagster/dbt.

1. **Pure / mostly-pure utilities**
  - `src/utils/polars_utils.py`: deterministic transforms (lags/leads/rolling stats/time features).

2. **Configuration behavior**
  - `src/config/settings.py`: default values, derived paths, and `ensure_directories()` behavior.

3. **Logging**
  - `src/utils/logging_utils.py`: JSON formatting structure and logger handler setup.

4. **Loader boundaries (unit level)**
  - `src/loaders/weather_loader.py`, `src/loaders/generation_loader.py`: the resource generators should yield dict records for provided parquet files.
  - `src/loaders/dlt_pipeline.py`: orchestration functions can be smoke-tested via mocking (avoid actually running dlt in unit tests).

### High-risk / special-handling areas
- **Global config import side effects**: `src/config/settings.py` instantiates `config = Config()` at import time. Tests should avoid relying on a real `.env` and should use `monkeypatch` to control environment variables when needed.
- **Filesystem paths are relative by default**: config defaults like `data/raw` assume repo-root working directory. Tests should prefer `tmp_path` and inject paths via env vars or by constructing `Config()` explicitly.
- **Dagster modules mutate `sys.path`**: Dagster packages in `dags/dagster_project/*` insert `src` into `sys.path` for imports. Keep unit tests focused on `src/` and reserve Dagster tests for integration phase.
- **dlt decorators**: `@dlt.resource` wraps generator functions. Unit tests should focus on the yielded records for a small parquet file and avoid running full pipelines until integration testing.
- **DuckDB integration**: correlation asset and verify_ingestion connect to a DuckDB file on disk; tests should use in-memory DuckDB or a temp database path.

### Concrete “Phase 0 done” checklist
- Confirm no existing python tests: there is currently no `tests/` directory.
- Confirm test tooling baseline exists:
  - pytest config is already present in `pyproject.toml`.
  - `pytest` and `pytest-mock` are already in the `dev` extra.
- Identify first-pass unit-test targets (above) and note the risks.

Next: follow the Phase 1 roadmap docs in `docs/planning/phase-1/` to add the missing scaffolding.

## Phase 1 — Test infra & conventions
- Goal: Add testing tools and define patterns.
- Deliverables:
  - Add `pytest`, `pytest-cov`, and `pytest-mock` to dev dependencies.
  - Create `tests/` tree and `tests/conftest.py` for shared fixtures.
  - Add `pyproject.toml` test scripts or `pytest.ini` config.
- Artifacts:
  - `tests/conftest.py`, `pytest.ini` or `pyproject.toml` test section.

## Phase 2 — Shared fixtures and test-data
- Goal: Build reliable, reusable fixtures.
- Deliverables:
  - `tmp_path`-based fixtures for filesystem operations.
  - In-memory DuckDB fixture for SQL/dbt integration testing.
  - Polars DataFrame factory helpers (small deterministic samples).
  - Simple sample CSV/Parquet files under `tests/data/` (small, committed seeds).
- Artifacts:
  - `tests/fixtures/*.py`, `tests/data/` sample files.

Implementation roadmap: see the Phase 2 playbook in `docs/planning/phase-2/`.

## Phase 3 — Unit tests (core modules)
- Goal: Validate pure logic and small units.
- Deliverables:
  - Unit tests for `src/utils/*` and `src/config/*`.
  - Tests for small helpers in `src/loaders` that can be run with mocks.
- Approach:
  - Use parametrized tests for edge cases.
  - Use `pytest-mock` or `monkeypatch` for environment variables and I/O.

Implementation roadmap: see the Phase 3 playbook in `docs/planning/phase-3/`.

## Phase 4 — Unit tests (I/O heavy modules)
- Goal: Test loaders and pipelines that interact with external systems.
- Deliverables:
  - Tests for `generation_loader.py` and `weather_loader.py` that mock network/API calls and use test data fixtures.
  - Tests for `dlt_pipeline.py` verifying orchestration logic with minimal side effects.
- Approach:
  - Replace network calls with fixtures returning deterministic data.
  - Use temporary directories and in-memory DuckDB for any DB writes.

Implementation roadmap: see the Phase 4 playbook in `docs/planning/phase-4/`.

## Phase 5 — Integration tests (dbt, Dagster, E2E)
- Goal: Verify cross-component functionality.
- Deliverables:
  - Small integration test that runs compiled dbt SQL (from `dbt/target/compiled/`) against an in-memory DuckDB and asserts expected rows/columns.
  - Dagster asset/job tests using Dagster test harness to run assets in-process with test resources.
  - One E2E smoke test: run minimal pipeline with sample data and assert expected outputs.
- Artifacts:
  - `tests/integration/test_dbt_smoke.py`
  - `tests/integration/test_dagster_assets_smoke.py`
  - `tests/integration/test_e2e_smoke.py`

Implementation roadmap: see the Phase 5 playbook in `docs/planning/phase-5/`.

Status: completed and verified (2025-12-20).

Note: full suite re-verified after the Python 3.12+ standardization on 2025-12-21.

## Phase 6 — CI, coverage and maintenance
- Goal: Enforce tests in CI and keep coverage healthy.
- Deliverables:
  - GitHub Actions workflow to run tests and report coverage.
  - Coverage threshold in `pytest.ini` or CI step (start with 60–70%).
  - Documentation in `docs/` describing how to run tests locally and in CI.

---

## Quick success criteria
- Tests run locally via `pytest` and in CI.
- Shared fixtures reduce duplication and flakiness.
- Integration tests exercise dbt compiled SQL and Dagster assets in lightweight mode.
- Coverage baseline established and enforced.


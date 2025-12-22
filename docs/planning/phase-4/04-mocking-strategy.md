# Phase 4.4 — Mocking Strategy

## Objective
Standardize how we patch loader modules so tests remain stable and easy to maintain.

## Preferred tools
- `pytest-mock` (`mocker`) for patching functions/classes.
- `monkeypatch` for environment variables and lightweight attribute replacement.

## What to patch (and where)
Patch the symbol **as imported by the module under test**, not the original location.

Examples:
- For `run_full_ingestion()` in `weather_adjusted_generation_analytics/loaders/dlt_pipeline.py`:
  - patch `src.loaders.dlt_pipeline.run_weather_ingestion`
  - patch `src.loaders.dlt_pipeline.run_generation_ingestion`
  - patch `src.loaders.dlt_pipeline.config.ensure_directories`

- For file discovery in `weather_loader.py`:
  - patch `src.loaders.weather_loader.config.weather_raw_path`

## How to avoid global config pitfalls
- Prefer passing explicit `file_paths=[...]` in tests.
- If you must test auto-discovery, patch the `config.*_raw_path` attributes to a temp directory.

## Fake objects patterns
For dlt pipeline mocks, use lightweight fakes:
- Fake pipeline object with `.pipeline_name` attribute and `.run(...)` method.
- Fake load_info with:
  - `.has_failed_jobs` boolean
  - `.load_packages` list containing `jobs["failed_jobs"]`

## Acceptance criteria
- Loader tests are independent and do not share state.
- Mocking is localized and readable.
- No test depends on local `.env` or repo `data/`.

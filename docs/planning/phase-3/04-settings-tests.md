# Phase 3.4 — `settings` Unit Tests

## Objective
Validate `src/config/settings.py` without relying on a real `.env`.

## Where tests should live
- `tests/unit/test_settings.py`

## Fixtures to use
- `temp_config` from `tests/conftest.py` (already constructs a `Config` rooted at `tmp_path`).

## Test plan

### Defaults and types
- Construct `Config()` directly (optional) and assert:
  - attribute types are `pathlib.Path` for paths
  - `duckdb_threads` is int
  - `log_format` is one of `"json" | "text"`

Note: if your local environment variables affect defaults, prefer explicit construction instead of `Config()`.

### Derived path properties
- For an explicitly constructed `Config`, assert:
  - `weather_raw_path == data_raw / "weather"`
  - `generation_raw_path == data_raw / "generation"`

### `ensure_directories()`
- Call `ensure_directories()` on `temp_config`.
- Assert the expected directories exist.
- Mark this test `@pytest.mark.io`.

### Boundary / validation behavior
- `mock_asset_count` has `ge=1, le=100`.
  - Construct `Config(mock_asset_count=0, ...)` and assert Pydantic raises.
  - Construct `Config(mock_asset_count=101, ...)` and assert raise.

## What to avoid
- Do not assert behavior tied to local `.env` files.
- Do not rely on the module-level `config = Config()` instance.

## Acceptance criteria
- Tests validate functional behavior (paths, directory creation, constraints).
- Tests are robust to developer machine configuration.

# Phase 4.2 — Weather & Generation Loader Tests

## Objective
Test parquet-reading and record-yielding behavior in `weather_loader.py` and `generation_loader.py`.

## Where tests should live
- `tests/unit/test_weather_loader.py`
- `tests/unit/test_generation_loader.py`

## Recommended fixtures
Use Phase 2 fixtures:
- `weather_parquet_path`, `generation_parquet_path` (temp parquet)
- `repo_weather_sample_parquet`, `repo_generation_sample_parquet` (committed parquet)

## Test plan

### Resource generator yields dicts
- Call `load_weather_parquet(file_paths=[...])`
- Convert iterator to list: `records = list(load_weather_parquet(file_paths=[path]))`
- Assert:
  - `len(records) > 0`
  - each `record` is a dict
  - required keys exist (`timestamp`, `asset_id`, `wind_speed_mps`, `ghi` for weather; `timestamp`, `asset_id`, `net_generation_mwh`, `asset_capacity_mw` for generation)

### Auto-discovery path (file_paths=None)
- Create a temp directory with expected file naming pattern:
  - `weather_*.parquet` and `generation_*.parquet`
- Patch `src.config.config.weather_raw_path` / `generation_raw_path` (or patch the module import’s `config`) to point at the temp directory.
- Call `load_weather_parquet()` with `None` and assert it finds the file.

### Error behavior
- Provide a non-existent file path and assert it raises.
- Optionally patch `polars.read_parquet` to raise and assert exception propagates.

## Notes
- These tests are I/O heavy (parquet) but should still be very fast due to small sample size.
- Mark these tests `@pytest.mark.unit` + `@pytest.mark.io`.

# Plan: Runtime Mock Data Generation

> Source spec: `docs/superpowers/specs/2026-04-11-runtime-mock-data-generation-design.md`

## Architectural Decisions

- **No parquet files:** Ingestion assets generate data in-memory and yield dicts directly to dlt — no filesystem dependency.
- **Daily partitions:** `DailyPartitionsDefinition(start_date="2023-01-01")` gives backfill support from the Dagster UI.
- **Partition key as date:** Each asset reads `context.partition_key` to determine which day to generate.
- **No random seed:** Generators called without `random_seed` so each run produces different data (simulates real-world variation).
- **Existing generators reused:** `generate_weather_data` and `generate_generation_data` from `mock_data/` called with a 1-day window.
- **Merge idempotency preserved:** dlt `write_disposition="merge"` on `(asset_id, timestamp)` means re-running a partition is safe. Note: without `random_seed`, re-runs produce different values — merge overwrites, no duplicates.
- **Empty DataFrame guard:** Raise `dagster.Failure` if generator returns 0 rows — zero silent failures.
- **Asset count sync:** Weather generator called with `asset_count=10` to match `len(ASSET_CONFIGS)` in generation generator.
- **Partition boundary:** Use `{partition_key}T00:00:00` to `{partition_key}T23:00:00` — produces exactly 24 hourly records per asset, no overlap with adjacent partitions.
- **Observability:** `context.log.info` for partition key, generated row count, and asset count.

---

## Phase 1: Partitioned Ingestion Assets

### What to build

Rewrite `generation.py` and `weather.py` to be daily-partitioned assets that generate mock data in-memory instead of reading parquet files from disk.

### Files modified

| File                                                   | Change                                                                                                        |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------- |
| `src/weather_analytics/assets/ingestion/generation.py` | Remove `Config`, remove file-based dlt resource, add `DailyPartitionsDefinition`, generate from partition key |
| `src/weather_analytics/assets/ingestion/weather.py`    | Same pattern as generation                                                                                    |

### Acceptance criteria

- [ ] Both assets have `partitions_def=DailyPartitionsDefinition(start_date="2023-01-01")`
- [ ] Both assets read `context.partition_key` to determine the target date
- [ ] `generation.py` calls `generate_generation_data(start, end)` with no `random_seed`
- [ ] `weather.py` calls `generate_weather_data(start, end)` with no `random_seed`
- [ ] dlt resource yields `df.to_dicts()` (no parquet files involved)
- [ ] `GenerationIngestionConfig` and `WeatherIngestionConfig` classes removed
- [ ] dlt resource names unchanged (`"generation"`, `"weather"`)
- [ ] `write_disposition="merge"` and `primary_key` unchanged
- [ ] Asset names, group, `op_tags` unchanged
- [ ] Asset raises `dagster.Failure` if generator returns empty DataFrame
- [ ] `context.log.info` logs partition key, row count, and asset count before dlt load
- [ ] Weather generator called with `asset_count=10` (matching `len(ASSET_CONFIGS)`)
- [ ] Partition boundary: `{key}T00:00:00` to `{key}T23:00:00` (24 hours, no overlap)
- [ ] `MaterializeResult` metadata still reports `load_id`, `rows_loaded`, etc.
- [ ] Unit test: valid partition key produces non-empty dlt load
- [ ] Unit test: empty generator output raises `dagster.Failure` (mock generator)
- [ ] Unit test: dlt resource yields correct record count (240 = 24h × 10 assets for generation)
- [ ] Existing tests pass: `uv run pytest -m unit -q`
- [ ] Lint clean: `uv run ruff check src/weather_analytics/`

---

## Phase 2: Partition-Aware Schedule

### What to build

Update `waga_daily_ingestion_schedule` in `schedules.py` to be partition-aware, materializing yesterday's partition at 06:00 UTC.

### Files modified

| File                                 | Change                                                                               |
| ------------------------------------ | ------------------------------------------------------------------------------------ |
| `src/weather_analytics/schedules.py` | Replace static `AssetSelection` schedule with partition-aware schedule for ingestion |

### Acceptance criteria

- [ ] Ingestion schedule materializes yesterday's partition daily at 06:00 UTC
- [ ] dbt and analytics schedules unchanged
- [ ] Schedule tests updated if any exist
- [ ] All tests pass: `uv run pytest -m unit -q`
- [ ] Lint clean: `uv run ruff check src/weather_analytics/`

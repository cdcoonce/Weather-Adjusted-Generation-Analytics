# Runtime Mock Data Generation

## Problem

Ingestion assets read parquet files from `data/raw/{weather,generation}/`, but these
files are gitignored and don't exist in the Dagster Cloud serverless environment.
Ingestion "succeeds" with zero rows, so no tables are created in Snowflake RAW.

## Solution

Replace file-based ingestion with in-memory mock data generation. Each ingestion
asset becomes a daily-partitioned asset that generates one day of synthetic data per
partition and loads it directly into Snowflake via dlt.

## Data Flow

```
Daily schedule (06:00 UTC) → materializes yesterday's partition
  OR
Backfill (Dagster UI) → materializes selected date range

  → asset reads context.partition_key ("2024-03-15")
  → calls generate_*_data("2024-03-15T00:00", "2024-03-15T23:00")
  → DataFrame.to_dicts() yields records to dlt resource
  → dlt merges into Snowflake RAW on (asset_id, timestamp)
```

## Design Details

### Partitioning

Both `waga_weather_ingestion` and `waga_generation_ingestion` get a
`DailyPartitionsDefinition` starting from `2023-01-01`. Each partition key is a
date string (e.g., `"2023-06-15"`).

At runtime the asset reads `context.partition_key` to determine which day to
generate. The daily schedule materializes yesterday's partition automatically.
Backfills target any date range through the Dagster UI.

### Ingestion Assets (generation.py, weather.py)

Changes per asset:

1. **Remove** `Config` class with `source_path` — no longer reading from disk.
2. **Remove** the `_*_dlt_resource` function that globs parquet files.
3. **Add** `DailyPartitionsDefinition` to the `@asset` decorator.
4. **Replace** with a new `_*_dlt_resource` that:
   - Reads `context.partition_key` to get the target date.
   - Calls `generate_*_data(start, end)` for that single day (no `random_seed`
     — each run produces different data simulating real-world variation).
   - Yields `df.to_dicts()` to dlt.
5. **Keep** asset name, group, `op_tags`, merge keys, metadata extraction.

### Schedule (schedules.py)

`waga_daily_ingestion_schedule` becomes partition-aware so it materializes
yesterday's partition at 06:00 UTC instead of triggering the assets generically.

### Mock Data Generators (mock_data/)

Used as-is. Called with a 1-day window per partition. The `random_seed` parameter
is not passed so each invocation uses entropy-based seeding.

## What Stays the Same

- dlt resource names (`"generation"`, `"weather"`) — Snowflake table names unchanged.
- `write_disposition="merge"` on `(asset_id, timestamp)` — idempotent re-runs.
- Asset names (`waga_weather_ingestion`, `waga_generation_ingestion`), groups, `op_tags`.
- All downstream assets (dbt, analytics), checks, and other schedules.
- Mock data generators in `src/weather_analytics/mock_data/`.

## Files Modified

| File                                                   | Change                                  |
| ------------------------------------------------------ | --------------------------------------- |
| `src/weather_analytics/assets/ingestion/generation.py` | Partitioned asset, in-memory generation |
| `src/weather_analytics/assets/ingestion/weather.py`    | Partitioned asset, in-memory generation |
| `src/weather_analytics/schedules.py`                   | Partition-aware ingestion schedule      |

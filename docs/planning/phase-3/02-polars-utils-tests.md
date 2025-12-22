# Phase 3.2 — `polars_utils` Unit Tests

## Objective
Add a comprehensive set of deterministic unit tests for `weather_adjusted_generation_analytics/utils/polars_utils.py`.

## Where tests should live
- `tests/unit/test_polars_utils.py`

## Fixtures to use
- Prefer generating small DataFrames directly in tests.
- For time series shapes, you can use the Phase 2 factories:
  - `weather_df` and `generation_df` fixtures (from `tests/conftest.py`)

## Test plan by function

### `add_lag_features(df, column, lags, partition_by=None)`
- Creates one column per lag: `${column}_lag_${lag}`
- Without `partition_by`:
  - For a simple ascending sequence, check values shift down by `lag`.
- With `partition_by`:
  - Use two assets; ensure lag does not bleed across assets.
- Edge cases:
  - `lags=[]` returns DataFrame with no additional columns.
  - A lag larger than group length yields nulls.

### `add_lead_features(df, column, leads, partition_by=None)`
- Similar assertions to lags, but shifting upward.

### `add_rolling_stats(df, column, window_sizes, stats=None, partition_by=None)`
- Default stats = `["mean", "std"]`.
- Verify expected column names: `${column}_rolling_${stat}_${window}`
- Value checks:
  - For a simple sequence, rolling mean for window 2 or 3 has known values.
- Partitioning:
  - Ensure rolling is calculated independently per asset when `partition_by` is used.
- Invalid stats behavior:
  - Current implementation silently skips unknown stats; test that unknown stats do not create columns.

### `calculate_correlation(df, col1, col2, window_size=None, partition_by=None)`
- Static correlation:
  - Without partition: returns a single-row DataFrame with column `corr_${col1}_${col2}`.
  - With partition: returns grouped correlation per asset.
- Rolling correlation:
  - Returns DataFrame with new column `corr_${col1}_${col2}_rolling_${window_size}`.
  - Basic sanity check: column exists and has nulls at the start.

### `add_time_features(df, timestamp_col="timestamp")`
- Verify columns exist: `hour`, `day`, `day_of_week`, `month`, `quarter`, `year`.
- For a known timestamp, assert exact extracted values.

### `calculate_capacity_factor(df, generation_col, capacity_col, hours=1.0)`
- For one row with known values, assert exact capacity factor.
- Assert `hours` affects denominator.

### `filter_by_date_range(df, start_date, end_date, timestamp_col="timestamp")`
- Ensure inclusive boundaries.
- Verify string dates work against datetime column (Polars should compare sensibly).

## Acceptance criteria
- Tests cover partitioned and non-partitioned paths where applicable.
- At least one test asserts actual numeric values (not just column existence).
- Tests remain deterministic and fast.

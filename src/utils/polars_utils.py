"""Polars utility functions for renewable energy data processing."""

from typing import Any

import polars as pl


def add_lag_features(
    df: pl.DataFrame,
    column: str,
    lags: list[int],
    partition_by: str | list[str] | None = None,
) -> pl.DataFrame:
    """
    Add lagged features for a specified column.

    Creates new columns with lagged values, useful for time series
    analysis and feature engineering. Handles partitioning by asset_id
    or other grouping columns.

    Parameters
    ----------
    df : pl.DataFrame
        Input dataframe
    column : str
        Column name to create lags for
    lags : list[int]
        List of lag periods (e.g., [1, 2, 24] for 1hr, 2hr, 24hr lags)
    partition_by : str | list[str], optional
        Column(s) to partition by (e.g., 'asset_id')

    Returns
    -------
    pl.DataFrame
        DataFrame with additional lag columns named {column}_lag_{n}

    Examples
    --------
    >>> df = pl.DataFrame({
    ...     "timestamp": [...],
    ...     "asset_id": [...],
    ...     "wind_speed_mps": [...]
    ... })
    >>> df_with_lags = add_lag_features(
    ...     df, "wind_speed_mps", [1, 2, 24], partition_by="asset_id"
    ... )

    """
    result = df.clone()

    for lag in lags:
        lag_col_name = f"{column}_lag_{lag}"

        if partition_by:
            result = result.with_columns(
                pl.col(column)
                .shift(lag)
                .over(partition_by)
                .alias(lag_col_name)
            )
        else:
            result = result.with_columns(
                pl.col(column).shift(lag).alias(lag_col_name)
            )

    return result


def add_lead_features(
    df: pl.DataFrame,
    column: str,
    leads: list[int],
    partition_by: str | list[str] | None = None,
) -> pl.DataFrame:
    """
    Add lead features for a specified column.

    Creates new columns with lead (future) values, useful for predictive
    modeling and time series analysis.

    Parameters
    ----------
    df : pl.DataFrame
        Input dataframe
    column : str
        Column name to create leads for
    leads : list[int]
        List of lead periods (e.g., [1, 2, 24] for 1hr, 2hr, 24hr leads)
    partition_by : str | list[str], optional
        Column(s) to partition by (e.g., 'asset_id')

    Returns
    -------
    pl.DataFrame
        DataFrame with additional lead columns named {column}_lead_{n}

    """
    result = df.clone()

    for lead in leads:
        lead_col_name = f"{column}_lead_{lead}"

        if partition_by:
            result = result.with_columns(
                pl.col(column)
                .shift(-lead)
                .over(partition_by)
                .alias(lead_col_name)
            )
        else:
            result = result.with_columns(
                pl.col(column).shift(-lead).alias(lead_col_name)
            )

    return result


def add_rolling_stats(
    df: pl.DataFrame,
    column: str,
    window_sizes: list[int],
    stats: list[str] | None = None,
    partition_by: str | list[str] | None = None,
) -> pl.DataFrame:
    """
    Add rolling window statistics for a specified column.

    Computes rolling statistics (mean, std, min, max) over specified
    window sizes. Useful for capturing short-term trends and variability.

    Parameters
    ----------
    df : pl.DataFrame
        Input dataframe
    column : str
        Column name to compute rolling stats for
    window_sizes : list[int]
        List of window sizes (e.g., [24, 168] for 24hr, 7-day windows)
    stats : list[str], optional
        Statistics to compute: "mean", "std", "min", "max".
        Defaults to ["mean", "std"]
    partition_by : str | list[str], optional
        Column(s) to partition by (e.g., 'asset_id')

    Returns
    -------
    pl.DataFrame
        DataFrame with additional rolling statistic columns

    Examples
    --------
    >>> df_with_rolling = add_rolling_stats(
    ...     df,
    ...     "wind_speed_mps",
    ...     window_sizes=[24, 168],
    ...     stats=["mean", "std"],
    ...     partition_by="asset_id"
    ... )

    """
    if stats is None:
        stats = ["mean", "std"]

    result = df.clone()

    for window in window_sizes:
        for stat in stats:
            col_name = f"{column}_rolling_{stat}_{window}"

            if stat == "mean":
                expr = pl.col(column).rolling_mean(window_size=window)
            elif stat == "std":
                expr = pl.col(column).rolling_std(window_size=window)
            elif stat == "min":
                expr = pl.col(column).rolling_min(window_size=window)
            elif stat == "max":
                expr = pl.col(column).rolling_max(window_size=window)
            else:
                continue

            if partition_by:
                expr = expr.over(partition_by)

            result = result.with_columns(expr.alias(col_name))

    return result


def calculate_correlation(
    df: pl.DataFrame,
    col1: str,
    col2: str,
    window_size: int | None = None,
    partition_by: str | list[str] | None = None,
) -> pl.DataFrame:
    """
    Calculate Pearson correlation between two columns.

    Computes either a static correlation or rolling window correlation.
    When window_size is specified, creates a new column with rolling
    correlation values.

    Parameters
    ----------
    df : pl.DataFrame
        Input dataframe
    col1 : str
        First column name
    col2 : str
        Second column name
    window_size : int, optional
        Window size for rolling correlation. If None, returns static value
    partition_by : str | list[str], optional
        Column(s) to partition by (e.g., 'asset_id')

    Returns
    -------
    pl.DataFrame
        If window_size is None, returns scalar correlation value.
        Otherwise, returns DataFrame with rolling correlation column.

    Examples
    --------
    >>> # Static correlation
    >>> corr = calculate_correlation(df, "wind_speed_mps", "net_generation_mwh")
    >>>
    >>> # Rolling 7-day correlation
    >>> df_with_corr = calculate_correlation(
    ...     df,
    ...     "wind_speed_mps",
    ...     "net_generation_mwh",
    ...     window_size=168,
    ...     partition_by="asset_id"
    ... )

    """
    if window_size is None:
        # Static correlation
        if partition_by:
            return df.group_by(partition_by).agg(
                pl.corr(col1, col2).alias(f"corr_{col1}_{col2}")
            )
        else:
            return df.select(
                pl.corr(col1, col2).alias(f"corr_{col1}_{col2}")
            )
    else:
        # Rolling correlation
        col_name = f"corr_{col1}_{col2}_rolling_{window_size}"

        # Polars doesn't have rolling_corr, so we use a custom approach
        result = df.clone()

        if partition_by:
            result = result.with_columns(
                pl.struct([col1, col2])
                .rolling_map(
                    lambda s: s.struct.field(col1).corr(s.struct.field(col2)),
                    window_size=window_size,
                )
                .over(partition_by)
                .alias(col_name)
            )
        else:
            result = result.with_columns(
                pl.struct([col1, col2])
                .rolling_map(
                    lambda s: s.struct.field(col1).corr(s.struct.field(col2)),
                    window_size=window_size,
                )
                .alias(col_name)
            )

        return result


def add_time_features(df: pl.DataFrame, timestamp_col: str = "timestamp") -> pl.DataFrame:
    """
    Extract time-based features from a timestamp column.

    Creates additional columns for hour, day, day_of_week, month,
    quarter, and year. Useful for temporal analysis and modeling.

    Parameters
    ----------
    df : pl.DataFrame
        Input dataframe
    timestamp_col : str, default="timestamp"
        Name of the timestamp column

    Returns
    -------
    pl.DataFrame
        DataFrame with additional time feature columns

    Examples
    --------
    >>> df_with_time = add_time_features(df)
    >>> # Adds: hour, day, day_of_week, month, quarter, year

    """
    return df.with_columns(
        [
            pl.col(timestamp_col).dt.hour().alias("hour"),
            pl.col(timestamp_col).dt.day().alias("day"),
            pl.col(timestamp_col).dt.weekday().alias("day_of_week"),
            pl.col(timestamp_col).dt.month().alias("month"),
            pl.col(timestamp_col).dt.quarter().alias("quarter"),
            pl.col(timestamp_col).dt.year().alias("year"),
        ]
    )


def calculate_capacity_factor(
    df: pl.DataFrame,
    generation_col: str,
    capacity_col: str,
    hours: float = 1.0,
) -> pl.DataFrame:
    """
    Calculate capacity factor for renewable energy assets.

    Capacity Factor = Generation (MWh) / (Capacity (MW) × Hours)

    Parameters
    ----------
    df : pl.DataFrame
        Input dataframe
    generation_col : str
        Column name containing generation in MWh
    capacity_col : str
        Column name containing asset capacity in MW
    hours : float, default=1.0
        Time period in hours (1.0 for hourly data)

    Returns
    -------
    pl.DataFrame
        DataFrame with additional 'capacity_factor' column

    Examples
    --------
    >>> df_with_cf = calculate_capacity_factor(
    ...     df,
    ...     generation_col="net_generation_mwh",
    ...     capacity_col="asset_capacity_mw",
    ...     hours=1.0
    ... )

    """
    return df.with_columns(
        (pl.col(generation_col) / (pl.col(capacity_col) * hours))
        .alias("capacity_factor")
    )


def filter_by_date_range(
    df: pl.DataFrame,
    start_date: str,
    end_date: str,
    timestamp_col: str = "timestamp",
) -> pl.DataFrame:
    """
    Filter dataframe by date range.

    Parameters
    ----------
    df : pl.DataFrame
        Input dataframe
    start_date : str
        Start date in YYYY-MM-DD format
    end_date : str
        End date in YYYY-MM-DD format
    timestamp_col : str, default="timestamp"
        Name of the timestamp column

    Returns
    -------
    pl.DataFrame
        Filtered dataframe

    """
    return df.filter(
        (pl.col(timestamp_col) >= start_date) &
        (pl.col(timestamp_col) <= end_date)
    )

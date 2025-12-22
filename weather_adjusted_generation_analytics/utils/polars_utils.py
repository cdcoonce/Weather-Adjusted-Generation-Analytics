"""Polars utility functions for renewable energy data processing."""

import polars as pl


def add_lag_features(
    df: pl.DataFrame,
    column: str,
    lags: list[int],
    partition_by: str | list[str] | None = None,
) -> pl.DataFrame:
    """Add lagged features for a specified column."""
    result = df.clone()

    for lag in lags:
        lag_col_name = f"{column}_lag_{lag}"
        if partition_by:
            result = result.with_columns(
                pl.col(column).shift(lag).over(partition_by).alias(lag_col_name)
            )
        else:
            result = result.with_columns(pl.col(column).shift(lag).alias(lag_col_name))

    return result


def add_lead_features(
    df: pl.DataFrame,
    column: str,
    leads: list[int],
    partition_by: str | list[str] | None = None,
) -> pl.DataFrame:
    """Add lead (future) features for a specified column."""
    result = df.clone()

    for lead in leads:
        lead_col_name = f"{column}_lead_{lead}"
        if partition_by:
            result = result.with_columns(
                pl.col(column).shift(-lead).over(partition_by).alias(lead_col_name)
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
    """Add rolling window statistics for a specified column."""
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
    """Calculate Pearson correlation between two columns."""
    if window_size is None:
        if partition_by:
            return df.group_by(partition_by).agg(
                pl.corr(col1, col2).alias(f"corr_{col1}_{col2}")
            )
        return df.select(pl.corr(col1, col2).alias(f"corr_{col1}_{col2}"))

    col_name = f"corr_{col1}_{col2}_rolling_{window_size}"
    result = df.clone()

    if partition_by:
        return result.with_columns(
            pl.struct([col1, col2])
            .rolling_map(
                lambda s: s.struct.field(col1).corr(s.struct.field(col2)),
                window_size=window_size,
            )
            .over(partition_by)
            .alias(col_name)
        )

    return result.with_columns(
        pl.struct([col1, col2])
        .rolling_map(
            lambda s: s.struct.field(col1).corr(s.struct.field(col2)),
            window_size=window_size,
        )
        .alias(col_name)
    )


def add_time_features(df: pl.DataFrame, timestamp_col: str = "timestamp") -> pl.DataFrame:
    """Extract time-based features from a timestamp column."""
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
    """Calculate capacity factor for renewable energy assets."""
    return df.with_columns(
        (
            pl.col(generation_col)
            / (pl.col(capacity_col) * pl.lit(hours))
        ).alias("capacity_factor")
    )


def filter_by_date_range(
    df: pl.DataFrame,
    timestamp_col: str = "timestamp",
    start_date: str | None = None,
    end_date: str | None = None,
) -> pl.DataFrame:
    """Filter a dataframe by an inclusive date range.

    Parameters
    ----------
    df:
        Input dataframe.
    timestamp_col:
        Name of the timestamp/date column to filter on.
    start_date:
        Inclusive lower bound (ISO-like string recommended).
    end_date:
        Inclusive upper bound (ISO-like string recommended).

    Returns
    -------
    pl.DataFrame
        Filtered dataframe.
    """
    result = df

    if start_date is not None:
        result = result.filter(pl.col(timestamp_col) >= pl.lit(start_date))
    if end_date is not None:
        result = result.filter(pl.col(timestamp_col) <= pl.lit(end_date))

    return result


__all__ = [
    "add_lag_features",
    "add_lead_features",
    "add_rolling_stats",
    "add_time_features",
    "calculate_capacity_factor",
    "calculate_correlation",
    "filter_by_date_range",
]


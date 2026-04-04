"""Polars utility functions for renewable energy data processing.

All functions accept and return ``pl.LazyFrame`` so that the caller
controls when computation is triggered via ``.collect()``.
"""

from __future__ import annotations

import polars as pl


def add_lag_features(
    lf: pl.LazyFrame,
    column: str,
    lags: list[int],
    partition_by: str | list[str] | None = None,
) -> pl.LazyFrame:
    """Add lagged features for a specified column.

    Parameters
    ----------
    lf
        Input lazy frame.
    column
        Column to compute lags on.
    lags
        List of positive lag offsets.
    partition_by
        Optional column(s) to partition the shift operation.

    Returns
    -------
    pl.LazyFrame
        Lazy frame with additional ``{column}_lag_{n}`` columns.
    """
    for lag in lags:
        lag_col_name = f"{column}_lag_{lag}"
        if partition_by:
            lf = lf.with_columns(
                pl.col(column).shift(lag).over(partition_by).alias(lag_col_name)
            )
        else:
            lf = lf.with_columns(pl.col(column).shift(lag).alias(lag_col_name))
    return lf


def add_lead_features(
    lf: pl.LazyFrame,
    column: str,
    leads: list[int],
    partition_by: str | list[str] | None = None,
) -> pl.LazyFrame:
    """Add lead (future) features for a specified column.

    Parameters
    ----------
    lf
        Input lazy frame.
    column
        Column to compute leads on.
    leads
        List of positive lead offsets.
    partition_by
        Optional column(s) to partition the shift operation.

    Returns
    -------
    pl.LazyFrame
        Lazy frame with additional ``{column}_lead_{n}`` columns.
    """
    for lead in leads:
        lead_col_name = f"{column}_lead_{lead}"
        if partition_by:
            lf = lf.with_columns(
                pl.col(column).shift(-lead).over(partition_by).alias(lead_col_name)
            )
        else:
            lf = lf.with_columns(pl.col(column).shift(-lead).alias(lead_col_name))
    return lf


def add_rolling_stats(
    lf: pl.LazyFrame,
    column: str,
    window_sizes: list[int],
    stats: list[str] | None = None,
    partition_by: str | list[str] | None = None,
) -> pl.LazyFrame:
    """Add rolling window statistics for a specified column.

    Parameters
    ----------
    lf
        Input lazy frame.
    column
        Column to compute rolling stats on.
    window_sizes
        List of rolling window sizes.
    stats
        Statistic names to compute. Supported: ``mean``, ``std``,
        ``min``, ``max``. Defaults to ``["mean", "std"]``.
    partition_by
        Optional column(s) to partition the rolling operation.

    Returns
    -------
    pl.LazyFrame
        Lazy frame with additional rolling statistic columns.
    """
    if stats is None:
        stats = ["mean", "std"]

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

            lf = lf.with_columns(expr.alias(col_name))

    return lf


def calculate_correlation(
    lf: pl.LazyFrame,
    col1: str,
    col2: str,
    window_size: int | None = None,
    partition_by: str | list[str] | None = None,
) -> pl.LazyFrame:
    """Calculate Pearson correlation between two columns.

    Parameters
    ----------
    lf
        Input lazy frame.
    col1
        First column for correlation.
    col2
        Second column for correlation.
    window_size
        If provided, compute a rolling correlation of this size.
    partition_by
        Optional column(s) to group by (static) or partition (rolling).

    Returns
    -------
    pl.LazyFrame
        Lazy frame with correlation result(s).
    """
    if window_size is None:
        if partition_by:
            return lf.group_by(partition_by).agg(
                pl.corr(col1, col2).alias(f"corr_{col1}_{col2}")
            )
        return lf.select(pl.corr(col1, col2).alias(f"corr_{col1}_{col2}"))

    col_name = f"corr_{col1}_{col2}_rolling_{window_size}"

    expr = pl.struct([col1, col2]).rolling_map(
        lambda s: s.struct.field(col1).corr(s.struct.field(col2)),
        window_size=window_size,
    )

    if partition_by:
        expr = expr.over(partition_by)

    return lf.with_columns(expr.alias(col_name))


def add_time_features(
    lf: pl.LazyFrame,
    timestamp_col: str = "timestamp",
) -> pl.LazyFrame:
    """Extract time-based features from a timestamp column.

    Parameters
    ----------
    lf
        Input lazy frame.
    timestamp_col
        Name of the datetime column.

    Returns
    -------
    pl.LazyFrame
        Lazy frame with ``hour``, ``day``, ``day_of_week``, ``month``,
        ``quarter``, and ``year`` columns appended.
    """
    return lf.with_columns(
        pl.col(timestamp_col).dt.hour().alias("hour"),
        pl.col(timestamp_col).dt.day().alias("day"),
        pl.col(timestamp_col).dt.weekday().alias("day_of_week"),
        pl.col(timestamp_col).dt.month().alias("month"),
        pl.col(timestamp_col).dt.quarter().alias("quarter"),
        pl.col(timestamp_col).dt.year().alias("year"),
    )


def calculate_capacity_factor(
    lf: pl.LazyFrame,
    generation_col: str,
    capacity_col: str,
    hours: float = 1.0,
) -> pl.LazyFrame:
    """Calculate capacity factor for renewable energy assets.

    Parameters
    ----------
    lf
        Input lazy frame.
    generation_col
        Column with actual generation values.
    capacity_col
        Column with nameplate capacity values.
    hours
        Time period in hours for capacity calculation.

    Returns
    -------
    pl.LazyFrame
        Lazy frame with ``capacity_factor`` column appended.
    """
    return lf.with_columns(
        (pl.col(generation_col) / (pl.col(capacity_col) * pl.lit(hours))).alias(
            "capacity_factor"
        )
    )


def filter_by_date_range(
    lf: pl.LazyFrame,
    timestamp_col: str = "timestamp",
    start_date: str | None = None,
    end_date: str | None = None,
) -> pl.LazyFrame:
    """Filter a lazy frame by an inclusive date range.

    Parameters
    ----------
    lf
        Input lazy frame.
    timestamp_col
        Name of the timestamp/date column to filter on.
    start_date
        Inclusive lower bound (ISO-like string recommended).
    end_date
        Inclusive upper bound (ISO-like string recommended).

    Returns
    -------
    pl.LazyFrame
        Filtered lazy frame.
    """
    if start_date is not None:
        lf = lf.filter(pl.col(timestamp_col) >= pl.lit(start_date))
    if end_date is not None:
        lf = lf.filter(pl.col(timestamp_col) <= pl.lit(end_date))
    return lf


__all__ = [
    "add_lag_features",
    "add_lead_features",
    "add_rolling_stats",
    "add_time_features",
    "calculate_capacity_factor",
    "calculate_correlation",
    "filter_by_date_range",
]

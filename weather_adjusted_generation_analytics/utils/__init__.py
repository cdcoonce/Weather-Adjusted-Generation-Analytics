"""Utilities shim for weather_adjusted_generation_analytics."""

from .logging_utils import get_logger, log_execution_time
from .polars_utils import (
    add_lag_features,
    add_lead_features,
    add_rolling_stats,
    add_time_features,
    calculate_capacity_factor,
    calculate_correlation,
    filter_by_date_range,
)

__all__ = [
    "get_logger",
    "log_execution_time",
    "add_lag_features",
    "add_lead_features",
    "add_rolling_stats",
    "add_time_features",
    "calculate_capacity_factor",
    "calculate_correlation",
    "filter_by_date_range",
]

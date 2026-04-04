"""Polars-based analytics assets writing to the ANALYTICS schema."""

from weather_analytics.assets.analytics.correlation import (
    waga_correlation_analysis,
)

__all__ = ["waga_correlation_analysis"]

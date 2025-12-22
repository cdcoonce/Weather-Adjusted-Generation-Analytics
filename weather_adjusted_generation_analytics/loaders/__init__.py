"""Loaders subpackage shim for weather_adjusted_generation_analytics."""

from .dlt_pipeline import (
    run_combined_pipeline,
    run_full_ingestion,
    verify_ingestion,
)
from .generation_loader import (
    get_generation_pipeline,
    load_generation_parquet,
    run_generation_ingestion,
)
from .weather_loader import (
    get_weather_pipeline,
    load_weather_parquet,
    run_weather_ingestion,
)

__all__ = [
    "get_generation_pipeline",
    "get_weather_pipeline",
    "load_generation_parquet",
    "load_weather_parquet",
    "run_combined_pipeline",
    "run_full_ingestion",
    "run_generation_ingestion",
    "run_weather_ingestion",
    "verify_ingestion",
]

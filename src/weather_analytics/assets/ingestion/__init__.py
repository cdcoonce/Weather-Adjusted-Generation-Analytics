"""Ingestion assets — dlt-based loading into Snowflake RAW schema."""

from weather_analytics.assets.ingestion.generation import waga_generation_ingestion
from weather_analytics.assets.ingestion.weather import waga_weather_ingestion

__all__ = ["waga_generation_ingestion", "waga_weather_ingestion"]

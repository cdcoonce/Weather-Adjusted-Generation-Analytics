"""Unit tests for weather sourcing (synthetic + Open-Meteo fallback)."""

from __future__ import annotations

from datetime import datetime

import numpy as np
import polars as pl
import pytest
import requests

from weather_analytics.mock_data import weather_sources
from weather_analytics.mock_data.fleet import FLEET
from weather_analytics.mock_data.weather_sources import (
    WEATHER_COLUMNS,
    fetch_open_meteo,
    get_weather,
    synthetic_weather,
)

pytestmark = pytest.mark.unit

_START = "2026-01-01T00:00:00"
_END = "2026-01-02T23:00:00"  # 2 days -> 48 hours


def test_synthetic_weather_schema_and_shape() -> None:
    df = synthetic_weather(FLEET, _START, _END, random_seed=1)
    assert tuple(df.columns) == WEATHER_COLUMNS
    assert df.height == 48 * len(FLEET)
    assert df.null_count().to_numpy().sum() == 0


def test_synthetic_weather_is_deterministic() -> None:
    a = synthetic_weather(FLEET, _START, _END, random_seed=7)
    b = synthetic_weather(FLEET, _START, _END, random_seed=7)
    assert a.equals(b)


def test_synthetic_ghi_zero_at_night_positive_at_noon() -> None:
    df = synthetic_weather(FLEET, _START, _END, random_seed=1)
    midnight = df.filter(pl.col("timestamp").dt.hour() == 0)["ghi"].to_numpy()
    noon = df.filter(pl.col("timestamp").dt.hour() == 12)["ghi"].to_numpy()
    assert np.all(midnight == 0.0)
    assert noon.mean() > 0.0


def test_get_weather_synthetic_when_use_real_false() -> None:
    df, source = get_weather(FLEET, _START, _END, use_real=False)
    assert source == "synthetic"
    assert df.height == 48 * len(FLEET)


def test_fetch_open_meteo_returns_none_on_network_error(monkeypatch) -> None:
    def _boom(*_args, **_kwargs):
        raise requests.RequestException

    monkeypatch.setattr(weather_sources.requests, "get", _boom)
    assert fetch_open_meteo(FLEET, _START, _END) is None


def test_get_weather_falls_back_to_synthetic_on_failure(monkeypatch) -> None:
    def _boom(*_args, **_kwargs):
        raise requests.RequestException

    monkeypatch.setattr(weather_sources.requests, "get", _boom)
    df, source = get_weather(FLEET, _START, _END, use_real=True)
    assert source == "synthetic"
    assert df.height == 48 * len(FLEET)


def test_synthetic_weather_day_slice_matches_standalone_day() -> None:
    """A day inside a multi-day window equals the same day generated alone."""
    window = weather_sources.synthetic_weather(
        FLEET, "2023-06-08T00:00:00", "2023-06-15T23:00:00", random_seed=42
    )
    day = weather_sources.synthetic_weather(
        FLEET, "2023-06-15T00:00:00", "2023-06-15T23:00:00", random_seed=42
    )
    sliced = window.filter(pl.col("timestamp") >= datetime(2023, 6, 15))
    assert sliced.equals(day)


def test_synthetic_weather_partial_day_window_consistent() -> None:
    """Partial-day windows reproduce the same values as full-day windows."""
    full = weather_sources.synthetic_weather(
        FLEET, "2023-06-15T00:00:00", "2023-06-15T23:00:00", random_seed=42
    )
    partial = weather_sources.synthetic_weather(
        FLEET, "2023-06-15T06:00:00", "2023-06-15T18:00:00", random_seed=42
    )
    expected = full.filter(
        (pl.col("timestamp") >= datetime(2023, 6, 15, 6))
        & (pl.col("timestamp") <= datetime(2023, 6, 15, 18))
    )
    assert partial.equals(expected)

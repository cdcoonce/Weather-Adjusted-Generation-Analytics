"""Unit tests for ``weather_analytics.dashboard.components.kpi_cards``.

Tests exercise the pure ``compute_kpis()`` function that drives KPI card
rendering. Panel widget construction is NOT tested here — only the data
logic.

Tested behaviours:
- ``compute_kpis`` returns all four expected keys
- Correct numeric computations on populated DataFrames
- "—" sentinel returned for each metric when the filtered result is empty
- Filtering by ``asset_id`` isolates a single asset's rows
- Filtering by ``asset_type`` isolates assets matching that type
- Date-range filtering returns only rows within [date_start, date_end]
- Combined filter (asset_id + date range) narrows correctly
- ``kpi_row`` is importable and callable (smoke test only — no Panel rendering)
"""

from __future__ import annotations

import polars as pl
import pytest

from weather_analytics.dashboard.components.kpi_cards import compute_kpis, kpi_row

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_DAILY_DF = pl.DataFrame(
    {
        "asset_id": ["WIND_001", "WIND_001", "SOLAR_001", "SOLAR_001"],
        "asset_type": ["Wind", "Wind", "Solar", "Solar"],
        "date": ["2025-01-01", "2025-01-02", "2025-01-01", "2025-01-02"],
        "total_net_generation_mwh": [100.0, 200.0, 50.0, 80.0],
        "daily_capacity_factor": [0.40, 0.50, 0.30, 0.35],
        "avg_availability_pct": [98.0, 99.0, 97.0, 96.0],
    }
)

_WEATHER_DF = pl.DataFrame(
    {
        "asset_id": ["WIND_001", "WIND_001", "SOLAR_001", "SOLAR_001"],
        "date": ["2025-01-01", "2025-01-02", "2025-01-01", "2025-01-02"],
        "performance_score": [0.80, 0.90, 0.70, 0.75],
    }
)

_EMPTY_DAILY = pl.DataFrame(
    {
        "asset_id": pl.Series([], dtype=pl.Utf8),
        "asset_type": pl.Series([], dtype=pl.Utf8),
        "date": pl.Series([], dtype=pl.Utf8),
        "total_net_generation_mwh": pl.Series([], dtype=pl.Float64),
        "daily_capacity_factor": pl.Series([], dtype=pl.Float64),
        "avg_availability_pct": pl.Series([], dtype=pl.Float64),
    }
)

_EMPTY_WEATHER = pl.DataFrame(
    {
        "asset_id": pl.Series([], dtype=pl.Utf8),
        "date": pl.Series([], dtype=pl.Utf8),
        "performance_score": pl.Series([], dtype=pl.Float64),
    }
)


# ===========================================================================
# Return structure
# ===========================================================================


@pytest.mark.unit
def test_compute_kpis_returns_all_keys() -> None:
    result = compute_kpis(_DAILY_DF, _WEATHER_DF, "All", "All", "", "")
    assert "total_mwh" in result
    assert "avg_capacity_factor" in result
    assert "avg_availability" in result
    assert "avg_performance_score" in result


# ===========================================================================
# "All" filter — aggregates across all assets and dates
# ===========================================================================


@pytest.mark.unit
def test_compute_kpis_total_mwh_all_assets() -> None:
    result = compute_kpis(_DAILY_DF, _WEATHER_DF, "All", "All", "", "")
    # 100 + 200 + 50 + 80 = 430
    assert result["total_mwh"] == "430.0"


@pytest.mark.unit
def test_compute_kpis_avg_capacity_factor_all_assets() -> None:
    result = compute_kpis(_DAILY_DF, _WEATHER_DF, "All", "All", "", "")
    # (0.40 + 0.50 + 0.30 + 0.35) / 4 = 0.3875
    expected = f"{(0.40 + 0.50 + 0.30 + 0.35) / 4:.4f}"
    assert result["avg_capacity_factor"] == expected


@pytest.mark.unit
def test_compute_kpis_avg_availability_all_assets() -> None:
    result = compute_kpis(_DAILY_DF, _WEATHER_DF, "All", "All", "", "")
    # (98 + 99 + 97 + 96) / 4 = 97.5
    assert result["avg_availability"] == "97.5"


@pytest.mark.unit
def test_compute_kpis_avg_performance_score_all_assets() -> None:
    result = compute_kpis(_DAILY_DF, _WEATHER_DF, "All", "All", "", "")
    # (0.80 + 0.90 + 0.70 + 0.75) / 4 = 0.7875
    expected = f"{(0.80 + 0.90 + 0.70 + 0.75) / 4:.4f}"
    assert result["avg_performance_score"] == expected


# ===========================================================================
# Empty DataFrame — returns "—" sentinel
# ===========================================================================


@pytest.mark.unit
def test_compute_kpis_empty_daily_total_mwh_is_dash() -> None:
    result = compute_kpis(_EMPTY_DAILY, _EMPTY_WEATHER, "All", "All", "", "")
    assert result["total_mwh"] == "—"


@pytest.mark.unit
def test_compute_kpis_empty_daily_avg_capacity_factor_is_dash() -> None:
    result = compute_kpis(_EMPTY_DAILY, _EMPTY_WEATHER, "All", "All", "", "")
    assert result["avg_capacity_factor"] == "—"


@pytest.mark.unit
def test_compute_kpis_empty_daily_avg_availability_is_dash() -> None:
    result = compute_kpis(_EMPTY_DAILY, _EMPTY_WEATHER, "All", "All", "", "")
    assert result["avg_availability"] == "—"


@pytest.mark.unit
def test_compute_kpis_empty_weather_performance_score_is_dash() -> None:
    result = compute_kpis(_EMPTY_DAILY, _EMPTY_WEATHER, "All", "All", "", "")
    assert result["avg_performance_score"] == "—"


# ===========================================================================
# asset_id filter
# ===========================================================================


@pytest.mark.unit
def test_compute_kpis_filters_by_asset_id() -> None:
    result = compute_kpis(_DAILY_DF, _WEATHER_DF, "WIND_001", "All", "", "")
    # Only WIND_001 rows: 100 + 200 = 300
    assert result["total_mwh"] == "300.0"


@pytest.mark.unit
def test_compute_kpis_asset_id_not_in_weather_returns_dash_for_score() -> None:
    """If filtered weather df is empty, performance score is '—'."""
    result = compute_kpis(_DAILY_DF, _EMPTY_WEATHER, "WIND_001", "All", "", "")
    assert result["avg_performance_score"] == "—"


# ===========================================================================
# asset_type filter
# ===========================================================================


@pytest.mark.unit
def test_compute_kpis_filters_by_asset_type_wind() -> None:
    result = compute_kpis(_DAILY_DF, _WEATHER_DF, "All", "Wind", "", "")
    # Only Wind rows: 100 + 200 = 300
    assert result["total_mwh"] == "300.0"


@pytest.mark.unit
def test_compute_kpis_filters_by_asset_type_solar() -> None:
    result = compute_kpis(_DAILY_DF, _WEATHER_DF, "All", "Solar", "", "")
    # Only Solar rows: 50 + 80 = 130
    assert result["total_mwh"] == "130.0"


# ===========================================================================
# Date-range filter
# ===========================================================================


@pytest.mark.unit
def test_compute_kpis_filters_by_date_start() -> None:
    result = compute_kpis(_DAILY_DF, _WEATHER_DF, "All", "All", "2025-01-02", "")
    # Only rows with date >= 2025-01-02: WIND_001@jan02=200, SOLAR_001@jan02=80
    assert result["total_mwh"] == "280.0"


@pytest.mark.unit
def test_compute_kpis_filters_by_date_end() -> None:
    result = compute_kpis(_DAILY_DF, _WEATHER_DF, "All", "All", "", "2025-01-01")
    # Only rows with date <= 2025-01-01: WIND_001@jan01=100, SOLAR_001@jan01=50
    assert result["total_mwh"] == "150.0"


@pytest.mark.unit
def test_compute_kpis_filters_by_date_range_combined() -> None:
    result = compute_kpis(
        _DAILY_DF, _WEATHER_DF, "All", "All", "2025-01-01", "2025-01-01"
    )
    # Only rows with date == 2025-01-01: 100 + 50 = 150
    assert result["total_mwh"] == "150.0"


# ===========================================================================
# Combined asset_id + date filter
# ===========================================================================


@pytest.mark.unit
def test_compute_kpis_combined_asset_and_date_filter() -> None:
    result = compute_kpis(
        _DAILY_DF, _WEATHER_DF, "WIND_001", "All", "2025-01-02", "2025-01-02"
    )
    # WIND_001 on 2025-01-02 only: 200
    assert result["total_mwh"] == "200.0"


# ===========================================================================
# kpi_row smoke test (importability)
# ===========================================================================


@pytest.mark.unit
def test_kpi_row_is_importable() -> None:
    """kpi_row must be importable without Panel raising errors at import time."""

    assert callable(kpi_row)

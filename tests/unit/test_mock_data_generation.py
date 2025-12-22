"""Unit tests for mock generation helper functions.

These tests validate the deterministic math helpers used by the mock-data generators.
They intentionally avoid exercising full data generation (which is covered separately).

"""

from __future__ import annotations

import numpy as np
import pytest

from weather_adjusted_generation_analytics.mock_data.generate_generation import (
    solar_power_output,
    wind_power_curve,
)


@pytest.mark.unit
def test_wind_power_curve_edge_points_cut_in_rated_cut_out() -> None:
    """Validate wind turbine cut-in/rated/cut-out behavior.

    The implementation uses:
    - cut-in: 3 m/s (power starts ramping)
    - rated: 12 m/s (full capacity)
    - cut-out: 25 m/s (shutdown to zero)

    """
    capacity_mw = 100.0
    wind_speed = np.array([0.0, 2.99, 3.0, 6.0, 12.0, 24.99, 25.0], dtype=float)

    power = wind_power_curve(wind_speed=wind_speed, capacity_mw=capacity_mw)

    expected_at_6 = capacity_mw * ((6.0 - 3.0) / 9.0) ** 3
    expected = np.array([0.0, 0.0, 0.0, expected_at_6, capacity_mw, capacity_mw, 0.0])

    np.testing.assert_allclose(power, expected, rtol=0.0, atol=1e-12)
    assert power.min() >= 0.0
    assert power.max() <= capacity_mw
    assert power.shape == wind_speed.shape


@pytest.mark.unit
def test_solar_power_output_scales_linearly_and_clips() -> None:
    """Validate solar output scaling at STC and clipping to [0, capacity]."""
    capacity_mw = 40.0
    ghi = np.array([-100.0, 0.0, 500.0, 1000.0, 1500.0], dtype=float)

    power = solar_power_output(ghi=ghi, capacity_mw=capacity_mw)

    # Efficiency factor is 0.85 in the implementation.
    expected = np.array(
        [
            0.0,
            0.0,
            (500.0 / 1000.0) * capacity_mw * 0.85,
            (1000.0 / 1000.0) * capacity_mw * 0.85,
            capacity_mw,
        ]
    )

    np.testing.assert_allclose(power, expected, rtol=0.0, atol=1e-12)
    assert power.min() >= 0.0
    assert power.max() <= capacity_mw
    assert power.shape == ghi.shape

"""Unit tests for the vectorized plant-physics models.

Validates the wind power curve, solar NOCT/clipping model, battery SOC
dispatch, gas merit-order dispatch, air density, and AR(1) noise against their
defining physical properties.
"""

from __future__ import annotations

import numpy as np
import pytest

from weather_analytics.mock_data import physics
from weather_analytics.mock_data.fleet import (
    BatteryParams,
    GasParams,
    SolarParams,
    WindParams,
)

pytestmark = pytest.mark.unit


def _rng() -> np.random.Generator:
    return np.random.default_rng(0)


def test_air_density_reference_conditions() -> None:
    rho = physics.air_density(np.array([15.0]), np.array([1013.25]))
    assert rho[0] == pytest.approx(1.225, abs=0.01)


def test_air_density_rises_when_colder() -> None:
    cold = physics.air_density(np.array([-10.0]), np.array([1013.0]))
    warm = physics.air_density(np.array([35.0]), np.array([1013.0]))
    assert cold[0] > warm[0]


def test_ar1_length_and_stationary_std() -> None:
    series = physics.ar1_noise(20000, phi=0.85, sigma_stationary=0.2, rng=_rng())
    assert series.shape == (20000,)
    assert np.std(series) == pytest.approx(0.2, abs=0.03)


def test_ar1_zero_length() -> None:
    assert physics.ar1_noise(0, 0.8, 0.1, _rng()).shape == (0,)


def test_wind_zero_below_cut_in_and_above_cut_out() -> None:
    params = WindParams(turbulence_intensity=0.0)
    temp = np.array([15.0, 15.0])
    press = np.array([1013.0, 1013.0])
    speeds = np.array([2.0, 30.0])  # below cut-in, above cut-out
    out = physics.wind_power_mwh(speeds, temp, press, 100.0, params, _rng())
    assert out[0] == pytest.approx(0.0)
    assert out[1] == pytest.approx(0.0)


def test_wind_rated_plateau_applies_losses() -> None:
    params = WindParams(turbulence_intensity=0.0)
    speed = np.array([15.0])  # between rated and cut-out -> full fraction
    out = physics.wind_power_mwh(
        speed, np.array([15.0]), np.array([1013.0]), 100.0, params, _rng()
    )
    # capacity * density(~1) * (1-wake)*avail = 100 * ~1 * 0.9 * 0.95
    assert out[0] == pytest.approx(100.0 * 0.855, rel=0.05)


def test_wind_monotonic_in_ramp_region() -> None:
    params = WindParams(turbulence_intensity=0.0)
    speeds = np.array([4.0, 6.0, 8.0, 10.0])
    temp = np.full(4, 15.0)
    press = np.full(4, 1013.0)
    out = physics.wind_power_mwh(speeds, temp, press, 100.0, params, _rng())
    assert np.all(np.diff(out) > 0)


def test_solar_zero_at_night() -> None:
    out = physics.solar_power_mwh(
        np.array([0.0]), np.array([20.0]), np.array([0.0]), 50.0,
        SolarParams(), _rng(),
    )
    assert out[0] == pytest.approx(0.0)


def test_solar_clips_at_ac_rating() -> None:
    out = physics.solar_power_mwh(
        np.array([2000.0]), np.array([25.0]), np.array([0.0]), 50.0,
        SolarParams(), _rng(),
    )
    assert out[0] == pytest.approx(50.0, rel=1e-6)


def test_solar_temperature_derate_reduces_output() -> None:
    hot = physics.solar_power_mwh(
        np.array([600.0]), np.array([45.0]), np.array([0.0]), 50.0,
        SolarParams(), _rng(),
    )
    cool = physics.solar_power_mwh(
        np.array([600.0]), np.array([5.0]), np.array([0.0]), 50.0,
        SolarParams(), _rng(),
    )
    assert hot[0] < cool[0]


def test_battery_soc_stays_in_window() -> None:
    signal = np.tile([0.1, 0.9], 200)  # alternate charge / discharge
    out = physics.battery_dispatch(signal, 50.0, BatteryParams(), 0.35, 0.65)
    assert out["soc_pct"].min() >= 10.0 - 1e-6
    assert out["soc_pct"].max() <= 95.0 + 1e-6


def test_battery_round_trip_loss() -> None:
    signal = np.tile([0.1, 0.9], 500)
    out = physics.battery_dispatch(signal, 50.0, BatteryParams(), 0.35, 0.65)
    # Energy discharged is less than energy charged (losses on both legs).
    assert out["discharge_mwh"].sum() < out["charge_mwh"].sum()
    assert np.allclose(out["net_mwh"], out["discharge_mwh"] - out["charge_mwh"])


def test_gas_forced_outage_zeros_output() -> None:
    params = GasParams(forced_outage_rate=1.0)  # always down
    signal = np.full(48, 0.99)
    out = physics.gas_dispatch(signal, 200.0, params, 0.3, _rng())
    assert np.all(out["net_mwh"] == 0.0)
    assert np.all(out["online"] == 0.0)


def test_gas_respects_min_stable_load() -> None:
    params = GasParams(forced_outage_rate=0.0, min_load_frac=0.4, ramp_frac_per_hr=1.0)
    signal = np.linspace(0.0, 1.0, 100)
    out = physics.gas_dispatch(signal, 200.0, params, 0.3, _rng())
    running = out["net_mwh"][out["net_mwh"] > 0]
    assert np.all(running >= 0.4 * 200.0 - 1e-6)


def test_gas_fuel_and_co2_track_output() -> None:
    params = GasParams(forced_outage_rate=0.0)
    signal = np.full(24, 0.9)
    out = physics.gas_dispatch(signal, 200.0, params, 0.3, _rng())
    assert np.all(out["co2_tonnes"][out["net_mwh"] > 0] > 0)
    assert np.all(out["fuel_mmbtu"][out["net_mwh"] > 0] > 0)

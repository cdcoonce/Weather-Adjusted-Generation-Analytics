"""Vectorized plant-physics models for the fleet simulator.

Pure functions over numpy arrays — no I/O, no global state — so they are cheap
to unit-test and reuse. Wind and solar are fully vectorized; battery and gas
dispatch are inherently sequential (state at hour *t* depends on *t-1*) and use
an explicit scan.

References (verified 2026-07): IEC 61400-12-1 air-density correction; Sandia
PVPMC NOCT cell-temperature model; PVWatts v8 14% system-loss default;
Ornstein-Uhlenbeck / AR(1) equivalence for autocorrelated wind noise; EIA fleet
capacity factors for sanity checks.
"""

from __future__ import annotations

import numpy as np

from weather_analytics.mock_data.fleet import (
    CO2_TONNES_PER_MMBTU,
    BatteryParams,
    GasParams,
    SolarParams,
    WindParams,
)

# Physical constants.
_R_SPECIFIC_DRY_AIR = 287.05  # J/(kg*K)
_RHO_REF = 1.225  # kg/m^3 at 15 C, sea level (IEC reference density)
_STC_IRRADIANCE = 1000.0  # W/m^2
_STC_CELL_TEMP_C = 25.0
_NOCT_IRRADIANCE = 800.0
_NOCT_AMBIENT_C = 20.0


def air_density(temperature_c: np.ndarray, pressure_hpa: np.ndarray) -> np.ndarray:
    """Air density (kg/m^3) from the ideal gas law.

    Parameters
    ----------
    temperature_c : np.ndarray
        Air temperature (°C).
    pressure_hpa : np.ndarray
        Surface pressure (hPa).

    Returns
    -------
    np.ndarray
        Air density (kg/m^3).
    """
    temp_k = temperature_c + 273.15
    pressure_pa = pressure_hpa * 100.0
    return pressure_pa / (_R_SPECIFIC_DRY_AIR * temp_k)


def ar1_noise(
    n: int,
    phi: float,
    sigma_stationary: float,
    rng: np.random.Generator,
) -> np.ndarray:
    """Generate an AR(1) (discretely-sampled Ornstein-Uhlenbeck) noise series.

    ``x_t = phi * x_{t-1} + eps_t`` with ``eps_t ~ N(0, sigma_eps^2)`` chosen so
    the stationary std equals ``sigma_stationary``. The initial sample is drawn
    from the stationary distribution to avoid a warm-up transient.

    Parameters
    ----------
    n : int
        Number of samples.
    phi : float
        Autocorrelation coefficient in (0, 1). Higher == more persistent.
    sigma_stationary : float
        Target stationary standard deviation of the series.
    rng : np.random.Generator
        Random source.

    Returns
    -------
    np.ndarray
        Length-``n`` autocorrelated noise series.
    """
    if n <= 0:
        return np.zeros(0)
    sigma_eps = sigma_stationary * np.sqrt(1.0 - phi**2)
    innovations = rng.normal(0.0, sigma_eps, n)
    out = np.empty(n)
    out[0] = rng.normal(0.0, sigma_stationary)
    for t in range(1, n):
        out[t] = phi * out[t - 1] + innovations[t]
    return out


def wind_power_mwh(
    wind_speed_mps: np.ndarray,
    temperature_c: np.ndarray,
    pressure_hpa: np.ndarray,
    capacity_mw: float,
    params: WindParams,
    rng: np.random.Generator,
    phi: float = 0.85,
) -> np.ndarray:
    """Wind farm hourly output (MWh) from a piecewise power curve.

    Applies AR(1) turbulence to the wind speed, the normalized cubic power
    curve, an air-density multiplier, and wake + availability losses.

    Parameters
    ----------
    wind_speed_mps : np.ndarray
        Hub-height wind speed (m/s).
    temperature_c, pressure_hpa : np.ndarray
        Weather used for the air-density correction.
    capacity_mw : float
        Nameplate capacity (MW).
    params : WindParams
        Power-curve and loss parameters.
    rng : np.random.Generator
        Random source for the turbulence noise.
    phi : float
        AR(1) persistence for the turbulence series.

    Returns
    -------
    np.ndarray
        Hourly generation (MWh) after losses.
    """
    n = wind_speed_mps.shape[0]
    turbulence = ar1_noise(n, phi, params.turbulence_intensity, rng)
    speed = np.clip(wind_speed_mps * (1.0 + turbulence), 0.0, None)

    cut_in, rated, cut_out = params.cut_in_mps, params.rated_mps, params.cut_out_mps
    fraction = np.zeros(n)

    ramp = (speed >= cut_in) & (speed < rated)
    fraction[ramp] = (speed[ramp] ** 3 - cut_in**3) / (rated**3 - cut_in**3)

    flat = (speed >= rated) & (speed <= cut_out)
    fraction[flat] = 1.0
    # Above cut-out and below cut-in stay 0.

    density_ratio = air_density(temperature_c, pressure_hpa) / _RHO_REF
    loss_factor = (1.0 - params.wake_loss) * params.availability
    return capacity_mw * fraction * density_ratio * loss_factor


def solar_power_mwh(
    ghi: np.ndarray,
    temperature_c: np.ndarray,
    cloud_cover_pct: np.ndarray,
    capacity_mw: float,
    params: SolarParams,
    rng: np.random.Generator,
    phi: float = 0.7,
) -> np.ndarray:
    """Solar PV hourly AC output (MWh) with NOCT derate and inverter clipping.

    ``capacity_mw`` is the AC (inverter) rating. DC nameplate is
    ``capacity_mw * dc_ac_ratio``; DC power above the AC rating is clipped.

    Parameters
    ----------
    ghi : np.ndarray
        Global horizontal irradiance (W/m^2).
    temperature_c : np.ndarray
        Ambient air temperature (°C).
    cloud_cover_pct : np.ndarray
        Total cloud cover (%), used for sub-hourly transient variability.
    capacity_mw : float
        AC (inverter) nameplate rating (MW).
    params : SolarParams
        PV system parameters.
    rng : np.random.Generator
        Random source for cloud-transient noise.
    phi : float
        AR(1) persistence for the cloud-transient series.

    Returns
    -------
    np.ndarray
        Hourly AC generation (MWh) after clipping and system derate.
    """
    n = ghi.shape[0]
    # Cloud-transient modulation: largest at partial cloud, ~0 when clear/overcast.
    cloud_frac = np.clip(cloud_cover_pct / 100.0, 0.0, 1.0)
    transient = np.clip(0.5 + ar1_noise(n, phi, 0.5, rng), 0.0, 1.0)
    ghi_eff = np.clip(ghi * (1.0 - 0.8 * cloud_frac * transient), 0.0, None)

    cell_temp = (
        temperature_c + ((params.noct_c - _NOCT_AMBIENT_C) / _NOCT_IRRADIANCE) * ghi_eff
    )
    temp_factor = 1.0 + params.temp_coeff_per_c * (cell_temp - _STC_CELL_TEMP_C)

    dc_nameplate = capacity_mw * params.dc_ac_ratio
    p_dc = dc_nameplate * (ghi_eff / _STC_IRRADIANCE) * temp_factor
    p_ac = np.minimum(p_dc * params.system_derate, capacity_mw)
    return np.clip(p_ac, 0.0, None)


def battery_dispatch(
    signal: np.ndarray,
    capacity_mw: float,
    params: BatteryParams,
    charge_threshold: float,
    discharge_threshold: float,
) -> dict[str, np.ndarray]:
    """Sequential SOC dispatch against a normalized price/net-load signal.

    Charges when the signal is below ``charge_threshold`` (cheap/surplus power)
    and discharges when above ``discharge_threshold`` (expensive/peak), subject
    to power, SOC-window, and round-trip-efficiency constraints. A small
    parasitic load is drawn every hour.

    Parameters
    ----------
    signal : np.ndarray
        Normalized dispatch signal in [0, 1] (e.g. percentile of net load).
    capacity_mw : float
        Rated power (MW).
    params : BatteryParams
        Storage parameters.
    charge_threshold, discharge_threshold : float
        Signal thresholds bounding the idle band.

    Returns
    -------
    dict[str, np.ndarray]
        ``charge_mwh``, ``discharge_mwh``, ``net_mwh`` (discharge - charge),
        and ``soc_pct`` (0-100) per hour.
    """
    n = signal.shape[0]
    energy_max = capacity_mw * params.duration_h
    soc_min = params.soc_min_frac * energy_max
    soc_max = params.soc_max_frac * energy_max
    eta = np.sqrt(params.round_trip_efficiency)  # symmetric charge/discharge leg
    aux = params.aux_load_frac * capacity_mw

    charge = np.zeros(n)
    discharge = np.zeros(n)
    soc_pct = np.zeros(n)
    soc = 0.5 * energy_max  # start half full

    for t in range(n):
        soc = max(soc - aux, soc_min)  # parasitic draw first
        if signal[t] < charge_threshold and soc < soc_max:
            headroom = (soc_max - soc) / eta
            power = min(capacity_mw, headroom)
            charge[t] = power
            soc += eta * power
        elif signal[t] > discharge_threshold and soc > soc_min:
            available = (soc - soc_min) * eta
            power = min(capacity_mw, available)
            discharge[t] = power
            soc -= power / eta
        soc = min(max(soc, soc_min), soc_max)
        soc_pct[t] = soc / energy_max * 100.0

    return {
        "charge_mwh": charge,
        "discharge_mwh": discharge,
        "net_mwh": discharge - charge,
        "soc_pct": soc_pct,
    }


def _part_load_heat_rate(load_frac: np.ndarray, params: GasParams) -> np.ndarray:
    """Heat rate (Btu/kWh) as a function of load fraction; convex penalty."""
    safe = np.where(load_frac > 0.0, load_frac, 1.0)
    return params.heat_rate_btu_kwh * (params.part_load_a + params.part_load_b / safe)


def gas_dispatch(
    signal: np.ndarray,
    capacity_mw: float,
    params: GasParams,
    dispatch_threshold: float,
    rng: np.random.Generator,
) -> dict[str, np.ndarray]:
    """Sequential merit-order gas dispatch with ramping, min load, and outages.

    Dispatches toward a target set by the residual-load/price ``signal`` when it
    clears ``dispatch_threshold``, honoring minimum stable load, hourly ramp
    limits, and stochastic forced outages. Computes fuel burn (part-load heat
    rate) and CO2.

    Parameters
    ----------
    signal : np.ndarray
        Normalized residual-load/price signal in [0, 1].
    capacity_mw : float
        Nameplate capacity (MW).
    params : GasParams
        Gas-unit parameters.
    dispatch_threshold : float
        Signal level above which the unit is called on.
    rng : np.random.Generator
        Random source for forced-outage sampling.

    Returns
    -------
    dict[str, np.ndarray]
        ``net_mwh``, ``fuel_mmbtu``, ``heat_rate_btu_kwh``, ``co2_tonnes``,
        and ``online`` (1.0 when not in a forced outage) per hour.
    """
    n = signal.shape[0]
    min_load = params.min_load_frac * capacity_mw
    ramp_limit = params.ramp_frac_per_hr * capacity_mw
    outage = np.asarray(rng.random(n) < params.forced_outage_rate)

    output = np.zeros(n)
    prev = 0.0
    for t in range(n):
        if outage[t]:
            target = 0.0
        elif signal[t] > dispatch_threshold:
            # Scale from min load to full capacity across the callable band.
            span = max(1.0 - dispatch_threshold, 1e-6)
            frac = (signal[t] - dispatch_threshold) / span
            target = min_load + frac * (capacity_mw - min_load)
        else:
            target = 0.0
        # Ramp constraint relative to previous hour.
        lo, hi = prev - ramp_limit, prev + ramp_limit
        power = float(np.clip(target, max(0.0, lo), hi))
        if 0.0 < power < min_load:
            power = 0.0  # never run below min stable load
        output[t] = power
        prev = power

    load_frac = np.where(capacity_mw > 0, output / capacity_mw, 0.0)
    heat_rate = np.where(
        output > 0, _part_load_heat_rate(load_frac, params), params.heat_rate_btu_kwh
    )
    fuel_mmbtu = output * heat_rate / 1000.0  # Btu/kWh * MWh = kBtu -> MMBtu/1000
    co2_tonnes = fuel_mmbtu * CO2_TONNES_PER_MMBTU
    online = np.where(outage, 0.0, 1.0)
    return {
        "net_mwh": output,
        "fuel_mmbtu": fuel_mmbtu,
        "heat_rate_btu_kwh": heat_rate,
        "co2_tonnes": co2_tonnes,
        "online": online,
    }


__all__ = [
    "air_density",
    "ar1_noise",
    "battery_dispatch",
    "gas_dispatch",
    "solar_power_mwh",
    "wind_power_mwh",
]

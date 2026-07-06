"""Realistic mixed-technology fleet registry.

Defines the assets the local simulation drives: onshore wind, utility solar PV,
grid-scale battery storage, and natural-gas generation (combined-cycle and
peaker). Each asset carries a real US latitude/longitude so the simulation can
pull genuine hourly weather from Open-Meteo and produce genuinely variable,
site-specific output.

This is the source of truth for the *local dashboard* fleet. The Snowflake
ingestion path keeps its own wind/solar-only ``ASSET_CONFIGS`` (see
``generate_generation.ASSET_CONFIGS``); the two are deliberately independent so
adding thermal/storage assets here never touches the contracted dbt marts.
"""

from __future__ import annotations

from dataclasses import dataclass

# Asset technology categories.
WIND = "wind"
SOLAR = "solar"
BATTERY = "battery"
GAS = "gas"

ASSET_TYPES: tuple[str, ...] = (WIND, SOLAR, BATTERY, GAS)


@dataclass(frozen=True)
class WindParams:
    """Turbine power-curve and loss parameters.

    Attributes
    ----------
    cut_in_mps, rated_mps, cut_out_mps : float
        Characteristic wind speeds (m/s) of the piecewise power curve.
    wake_loss : float
        Fractional array wake loss (0.10 == 10% lost).
    availability : float
        Mechanical availability factor (0.95 == 95% uptime).
    turbulence_intensity : float
        Stationary std of the multiplicative AR(1) wind-speed noise.
    """

    cut_in_mps: float = 3.0
    rated_mps: float = 12.0
    cut_out_mps: float = 25.0
    wake_loss: float = 0.10
    availability: float = 0.95
    turbulence_intensity: float = 0.12


@dataclass(frozen=True)
class SolarParams:
    """PV system parameters (NOCT cell-temp + inverter-clipping model).

    Attributes
    ----------
    noct_c : float
        Nominal operating cell temperature (°C).
    temp_coeff_per_c : float
        Power temperature coefficient (per °C, negative), referenced to 25 °C.
    dc_ac_ratio : float
        Inverter load ratio; AC rating = DC nameplate / dc_ac_ratio.
    system_derate : float
        Combined DC-side system losses factor (0.86 == 14% loss, PVWatts v8).
    """

    noct_c: float = 45.0
    temp_coeff_per_c: float = -0.004
    dc_ac_ratio: float = 1.2
    system_derate: float = 0.86


@dataclass(frozen=True)
class BatteryParams:
    """Grid Li-ion storage parameters.

    Attributes
    ----------
    duration_h : float
        Storage duration at rated power (energy = power * duration).
    round_trip_efficiency : float
        AC-to-AC round-trip efficiency; split symmetrically across legs.
    soc_min_frac, soc_max_frac : float
        Usable state-of-charge window as fractions of energy capacity.
    aux_load_frac : float
        Hourly parasitic/auxiliary draw as a fraction of rated power.
    """

    duration_h: float = 4.0
    round_trip_efficiency: float = 0.88
    soc_min_frac: float = 0.10
    soc_max_frac: float = 0.95
    aux_load_frac: float = 0.003


@dataclass(frozen=True)
class GasParams:
    """Natural-gas unit parameters (CCGT or simple-cycle peaker).

    Attributes
    ----------
    subtype : str
        ``"ccgt"`` or ``"peaker"`` — sets merit-order position.
    heat_rate_btu_kwh : float
        Full-load heat rate (Btu/kWh); lower == more efficient.
    min_load_frac : float
        Minimum stable load as a fraction of capacity.
    ramp_frac_per_hr : float
        Max change in output per hour as a fraction of capacity.
    forced_outage_rate : float
        Equivalent forced-outage rate (EFOR); hourly trip probability.
    part_load_a, part_load_b : float
        Part-load heat-rate curve HR(x) = HR_full * (a + b/x); a + b == 1.
    """

    subtype: str = "ccgt"
    heat_rate_btu_kwh: float = 6900.0
    min_load_frac: float = 0.40
    ramp_frac_per_hr: float = 0.80
    forced_outage_rate: float = 0.03
    part_load_a: float = 0.90
    part_load_b: float = 0.10


# Emissions factor for natural gas combustion (tonnes CO2 per MMBtu of fuel).
# EPA: ~53.06 kg CO2 / MMBtu for natural gas.
CO2_TONNES_PER_MMBTU: float = 0.05306


@dataclass(frozen=True)
class FleetAsset:
    """One physical asset in the simulated fleet.

    Attributes
    ----------
    asset_id : str
        Stable identifier (``ASSET_0NN``) matching the export/UI convention.
    name : str
        Human-readable site name.
    asset_type : str
        One of :data:`ASSET_TYPES`.
    capacity_mw : float
        Nameplate power capacity (MW). For solar this is the AC/inverter rating.
    latitude, longitude : float
        Site coordinates (WGS84) used for the weather pull.
    region : str
        Grid/region label for grouping in the dashboard.
    wind, solar, battery, gas : params | None
        Exactly one is populated, matching ``asset_type``.
    """

    asset_id: str
    name: str
    asset_type: str
    capacity_mw: float
    latitude: float
    longitude: float
    region: str
    wind: WindParams | None = None
    solar: SolarParams | None = None
    battery: BatteryParams | None = None
    gas: GasParams | None = None

    @property
    def size_category(self) -> str:
        """Bucket capacity into Small/Medium/Large (matches dbt staging)."""
        if self.capacity_mw >= 75:
            return "Large"
        if self.capacity_mw >= 50:
            return "Medium"
        return "Small"

    @property
    def display_name(self) -> str:
        """Label shown in the dashboard asset picker."""
        return f"{self.name} ({self.capacity_mw:.0f} MW {self.asset_type})"


def _wind(
    asset_id: str,
    name: str,
    mw: float,
    lat: float,
    lon: float,
    region: str,
    **kw: float,
) -> FleetAsset:
    params = WindParams(**kw)
    return FleetAsset(asset_id, name, WIND, mw, lat, lon, region, wind=params)


def _solar(
    asset_id: str,
    name: str,
    mw: float,
    lat: float,
    lon: float,
    region: str,
    **kw: float,
) -> FleetAsset:
    params = SolarParams(**kw)
    return FleetAsset(asset_id, name, SOLAR, mw, lat, lon, region, solar=params)


def _battery(
    asset_id: str,
    name: str,
    mw: float,
    lat: float,
    lon: float,
    region: str,
    **kw: float,
) -> FleetAsset:
    params = BatteryParams(**kw)
    return FleetAsset(asset_id, name, BATTERY, mw, lat, lon, region, battery=params)


def _gas(
    asset_id: str,
    name: str,
    mw: float,
    lat: float,
    lon: float,
    region: str,
    **kw: object,
) -> FleetAsset:
    params = GasParams(**kw)  # type: ignore[arg-type]
    return FleetAsset(asset_id, name, GAS, mw, lat, lon, region, gas=params)


# ---------------------------------------------------------------------------
# The fleet: 12 assets across four technologies at real US sites.
# Coordinates chosen for representative resource quality (windy plains, sunny
# Southwest) so the Open-Meteo pull yields believable, site-specific profiles.
# ---------------------------------------------------------------------------
FLEET: tuple[FleetAsset, ...] = (
    # --- Onshore wind: high-wind interior sites ---
    _wind("ASSET_001", "Roscoe Ridge", 150.0, 32.45, -100.54, "ERCOT"),
    _wind("ASSET_002", "Buffalo Ridge", 100.0, 44.30, -96.20, "MISO"),
    _wind(
        "ASSET_003",
        "Cheyenne Mesa",
        120.0,
        41.14,
        -104.82,
        "WECC",
        turbulence_intensity=0.14,
    ),
    _wind("ASSET_004", "Storm Lake", 80.0, 42.64, -95.20, "MISO"),
    # --- Utility solar PV: Southwest high-irradiance sites ---
    _solar("ASSET_005", "Mojave Flats", 90.0, 35.01, -117.30, "CAISO"),
    _solar("ASSET_006", "Gila Bend", 75.0, 32.95, -112.72, "WECC"),
    _solar("ASSET_007", "Pecos Plains", 60.0, 31.42, -103.49, "ERCOT"),
    _solar("ASSET_008", "Boulder Basin", 45.0, 35.98, -114.84, "WECC", dc_ac_ratio=1.3),
    # --- Grid battery storage ---
    _battery("ASSET_009", "Moss Landing Cells", 100.0, 36.80, -121.78, "CAISO"),
    _battery(
        "ASSET_010", "Angleton Store", 60.0, 29.17, -95.43, "ERCOT", duration_h=2.0
    ),
    # --- Natural gas ---
    _gas(
        "ASSET_011",
        "Deer Park CCGT",
        200.0,
        29.70,
        -95.12,
        "ERCOT",
        subtype="ccgt",
        heat_rate_btu_kwh=6900.0,
        min_load_frac=0.40,
        ramp_frac_per_hr=0.80,
        forced_outage_rate=0.03,
    ),
    _gas(
        "ASSET_012",
        "Sunrise Peaker",
        90.0,
        35.35,
        -119.02,
        "CAISO",
        subtype="peaker",
        heat_rate_btu_kwh=10500.0,
        min_load_frac=0.30,
        ramp_frac_per_hr=3.0,
        forced_outage_rate=0.04,
    ),
)

FLEET_BY_ID: dict[str, FleetAsset] = {a.asset_id: a for a in FLEET}


def assets_of_type(asset_type: str) -> list[FleetAsset]:
    """Return all fleet assets of a given technology."""
    return [a for a in FLEET if a.asset_type == asset_type]


__all__ = [
    "ASSET_TYPES",
    "BATTERY",
    "CO2_TONNES_PER_MMBTU",
    "FLEET",
    "FLEET_BY_ID",
    "GAS",
    "SOLAR",
    "WIND",
    "BatteryParams",
    "FleetAsset",
    "GasParams",
    "SolarParams",
    "WindParams",
    "assets_of_type",
]

"""Weather inputs for the fleet simulator: real (Open-Meteo) or synthetic.

The simulator is driven by one hourly weather frame covering every asset site.
When network access is available, :func:`fetch_open_meteo` pulls genuine ERA5
reanalysis hourly data from the free Open-Meteo archive API (no key). When it is
not — offline CI, air-gapped runs, an API hiccup — :func:`synthetic_weather`
produces a latitude-aware physical stand-in so the pipeline always runs.

Both return the same schema::

    timestamp, asset_id, wind_speed_mps, wind_direction_deg, ghi,
    temperature_c, pressure_hpa, relative_humidity, cloud_cover_pct
"""

from __future__ import annotations

import logging
from datetime import datetime

import numpy as np
import polars as pl
import requests  # type: ignore[import-untyped]

from weather_analytics.mock_data.fleet import FleetAsset

logger = logging.getLogger(__name__)

_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
_HOURLY_VARS = (
    "wind_speed_100m",
    "wind_direction_100m",
    "shortwave_radiation",
    "temperature_2m",
    "surface_pressure",
    "relative_humidity_2m",
    "cloud_cover",
)

WEATHER_COLUMNS: tuple[str, ...] = (
    "timestamp",
    "asset_id",
    "wind_speed_mps",
    "wind_direction_deg",
    "ghi",
    "temperature_c",
    "pressure_hpa",
    "relative_humidity",
    "cloud_cover_pct",
)


def _fetch_one(
    asset: FleetAsset, start_date: str, end_date: str, timeout: float
) -> pl.DataFrame:
    """Fetch one asset's hourly archive series and map to the local schema."""
    params = {
        "latitude": asset.latitude,
        "longitude": asset.longitude,
        "start_date": start_date[:10],
        "end_date": end_date[:10],
        "hourly": ",".join(_HOURLY_VARS),
        "wind_speed_unit": "ms",
        "timezone": "auto",
    }
    response = requests.get(_ARCHIVE_URL, params=params, timeout=timeout)
    response.raise_for_status()
    hourly = response.json()["hourly"]

    times = [datetime.fromisoformat(t) for t in hourly["time"]]

    def _col(key: str, default: float) -> list[float]:
        return [default if v is None else float(v) for v in hourly[key]]

    return pl.DataFrame(
        {
            "timestamp": times,
            "asset_id": [asset.asset_id] * len(times),
            "wind_speed_mps": _col("wind_speed_100m", 0.0),
            "wind_direction_deg": _col("wind_direction_100m", 0.0),
            "ghi": _col("shortwave_radiation", 0.0),
            "temperature_c": _col("temperature_2m", 15.0),
            "pressure_hpa": _col("surface_pressure", 1013.0),
            "relative_humidity": _col("relative_humidity_2m", 60.0),
            "cloud_cover_pct": _col("cloud_cover", 0.0),
        }
    )


def fetch_open_meteo(
    assets: tuple[FleetAsset, ...] | list[FleetAsset],
    start_date: str,
    end_date: str,
    timeout: float = 30.0,
) -> pl.DataFrame | None:
    """Fetch real hourly weather for every asset from the Open-Meteo archive.

    Parameters
    ----------
    assets : sequence of FleetAsset
        Fleet whose coordinates drive the per-site requests.
    start_date, end_date : str
        ISO datetimes (only the date portion is used by the archive API).
    timeout : float
        Per-request timeout in seconds.

    Returns
    -------
    pl.DataFrame | None
        Combined weather frame, or ``None`` if any request fails (caller falls
        back to synthetic weather).
    """
    frames: list[pl.DataFrame] = []
    try:
        for asset in assets:
            frames.append(_fetch_one(asset, start_date, end_date, timeout))
    except (requests.RequestException, KeyError, ValueError) as exc:
        logger.warning("Open-Meteo fetch failed (%s); using synthetic weather", exc)
        return None
    if not frames:
        return None
    logger.info("Fetched real Open-Meteo weather for %d assets", len(frames))
    return pl.concat(frames).select(WEATHER_COLUMNS).sort(["timestamp", "asset_id"])


def synthetic_weather(
    assets: tuple[FleetAsset, ...] | list[FleetAsset],
    start_date: str,
    end_date: str,
    random_seed: int = 42,
) -> pl.DataFrame:
    """Latitude-aware synthetic hourly weather (offline fallback).

    Physically-plausible seasonal + diurnal signals with autocorrelated noise,
    parameterized by each site's latitude so Southwest solar sites are hotter
    and sunnier than northern wind sites.

    Parameters
    ----------
    assets : sequence of FleetAsset
        Fleet to generate weather for.
    start_date, end_date : str
        ISO datetimes (inclusive) at hourly resolution.
    random_seed : int
        Seed for reproducibility.

    Returns
    -------
    pl.DataFrame
        Weather frame in :data:`WEATHER_COLUMNS` order.
    """
    rng = np.random.default_rng(random_seed)
    timestamps = pl.datetime_range(
        start=datetime.fromisoformat(start_date),
        end=datetime.fromisoformat(end_date),
        interval="1h",
        eager=True,
    )
    hours = timestamps.dt.hour().to_numpy()
    doy = timestamps.dt.ordinal_day().to_numpy()
    n = len(timestamps)

    frames: list[pl.DataFrame] = []
    for asset in assets:
        lat = asset.latitude
        # Clear-sky GHI from solar geometry: declination + zenith angle. This
        # gives physically-correct seasonal daylight length and peak irradiance
        # (short, weak winter days; long, strong summer days) per latitude.
        declination = np.radians(
            23.45 * np.sin(np.radians(360.0 * (284 + doy) / 365.0))
        )
        hour_angle = np.radians(15.0 * (hours - 12.0))
        lat_rad = np.radians(lat)
        cos_zenith = np.clip(
            np.sin(lat_rad) * np.sin(declination)
            + np.cos(lat_rad) * np.cos(declination) * np.cos(hour_angle),
            0.0,
            None,
        )
        ghi_clear = 1050.0 * cos_zenith  # clear-sky peak ~1050 W/m^2
        cloud = np.clip(
            35 + 25 * np.sin(2 * np.pi * doy / 365 + lat) + rng.normal(0, 18, n),
            0,
            100,
        )
        ghi = np.clip(ghi_clear * (1 - 0.7 * cloud / 100), 0, None)

        # Wind: windier at higher latitudes/plains, seasonal + diurnal + noise.
        base_wind = 6.5 + 0.06 * abs(lat - 25)
        seasonal_wind = base_wind + 2.5 * np.sin(2 * np.pi * (doy - 30) / 365)
        diurnal_wind = 1.5 * np.sin(2 * np.pi * (hours - 15) / 24)
        wind = np.clip(seasonal_wind + diurnal_wind + rng.normal(0, 2.5, n), 0.0, 28.0)
        wind_dir = (rng.uniform(0, 360) + rng.normal(0, 25, n)) % 360

        # Temperature: warmer at lower latitudes.
        base_temp = 28 - 0.55 * (lat - 25)
        temp = (
            base_temp
            + 12 * np.sin(2 * np.pi * (doy - 110) / 365)
            + 6 * np.sin(2 * np.pi * (hours - 15) / 24)
            + rng.normal(0, 2.2, n)
        )
        pressure = 1013 + 5 * np.sin(2 * np.pi * doy / 365) + rng.normal(0, 5, n)
        humidity = np.clip(
            62 - 0.4 * (temp - 15) - 0.012 * ghi + rng.normal(0, 9, n), 15, 98
        )

        frames.append(
            pl.DataFrame(
                {
                    "timestamp": timestamps,
                    "asset_id": [asset.asset_id] * n,
                    "wind_speed_mps": wind,
                    "wind_direction_deg": wind_dir,
                    "ghi": ghi,
                    "temperature_c": temp,
                    "pressure_hpa": pressure,
                    "relative_humidity": humidity,
                    "cloud_cover_pct": cloud,
                }
            )
        )
    return pl.concat(frames).select(WEATHER_COLUMNS).sort(["timestamp", "asset_id"])


def get_weather(
    assets: tuple[FleetAsset, ...] | list[FleetAsset],
    start_date: str,
    end_date: str,
    use_real: bool = True,
    random_seed: int = 42,
) -> tuple[pl.DataFrame, str]:
    """Return hourly weather and its provenance label.

    Tries Open-Meteo when ``use_real`` is set, falling back to synthetic weather
    on any failure so the caller always gets a usable frame.

    Returns
    -------
    tuple[pl.DataFrame, str]
        ``(weather_df, source)`` where source is ``"open-meteo"`` or
        ``"synthetic"``.
    """
    if use_real:
        real = fetch_open_meteo(assets, start_date, end_date)
        if real is not None and real.height > 0:
            return real, "open-meteo"
    return synthetic_weather(assets, start_date, end_date, random_seed), "synthetic"


__all__ = [
    "WEATHER_COLUMNS",
    "fetch_open_meteo",
    "get_weather",
    "synthetic_weather",
]

"""Deterministic Polars DataFrame factories for tests.

These factories create small, predictable datasets that match the schemas
expected by loaders and downstream analytics code.

"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

import polars as pl


@dataclass(frozen=True, slots=True)
class TimeRangeSpec:
    """Specification for a deterministic time range.

    Parameters
    ----------
    start : datetime
        Start timestamp (naive datetime expected).
    periods : int
        Number of periods to generate.
    step : timedelta
        Time step between periods.

    """

    start: datetime
    periods: int
    step: timedelta


def _timestamps(spec: TimeRangeSpec) -> list[datetime]:
    """Generate timestamps according to `spec`.

    Parameters
    ----------
    spec : TimeRangeSpec
        Time range specification.

    Returns
    -------
    list[datetime]
        List of timestamps.

    """
    if spec.periods <= 0:
        msg = "periods must be positive"
        raise ValueError(msg)

    return [spec.start + i * spec.step for i in range(spec.periods)]


def weather_df_small(
    *,
    start: datetime = datetime(2023, 1, 1, 0, 0, 0),
    periods: int = 24,
    step: timedelta = timedelta(hours=1),
    asset_ids: list[str] | None = None,
) -> pl.DataFrame:
    """Create a deterministic weather dataset.

    Schema includes the minimal columns used in loaders and correlation logic.

    Parameters
    ----------
    start : datetime, default=datetime(2023, 1, 1, 0, 0, 0)
        Start timestamp.
    periods : int, default=24
        Number of rows per asset.
    step : timedelta, default=timedelta(hours=1)
        Time delta between rows.
    asset_ids : list[str] | None, default=None
        Asset IDs to generate. Defaults to two assets.

    Returns
    -------
    pl.DataFrame
        Weather DataFrame.

    """
    if asset_ids is None:
        asset_ids = ["asset_001", "asset_002"]

    spec = TimeRangeSpec(start=start, periods=periods, step=step)
    timestamps = _timestamps(spec)

    rows: list[dict[str, object]] = []
    for asset_index, asset_id in enumerate(asset_ids):
        for time_index, ts in enumerate(timestamps):
            # Values are simple and deterministic; easy to assert in tests.
            wind_speed = 5.0 + asset_index + (time_index * 0.1)
            ghi = 100.0 + (time_index * 2.0)
            rows.append(
                {
                    "timestamp": ts,
                    "asset_id": asset_id,
                    "wind_speed_mps": float(wind_speed),
                    "ghi": float(ghi),
                    "temperature_c": 15.0 + (time_index % 5),
                    "pressure_hpa": 1013.25,
                    "relative_humidity": 0.50,
                }
            )

    return pl.DataFrame(rows).with_columns(
        [
            pl.col("timestamp").cast(pl.Datetime),
            pl.col("asset_id").cast(pl.Utf8),
        ]
    )


def generation_df_small(
    *,
    start: datetime = datetime(2023, 1, 1, 0, 0, 0),
    periods: int = 24,
    step: timedelta = timedelta(hours=1),
    asset_ids: list[str] | None = None,
) -> pl.DataFrame:
    """Create a deterministic generation dataset.

    Schema includes the minimal columns used in loaders and correlation logic.

    Parameters
    ----------
    start : datetime, default=datetime(2023, 1, 1, 0, 0, 0)
        Start timestamp.
    periods : int, default=24
        Number of rows per asset.
    step : timedelta, default=timedelta(hours=1)
        Time delta between rows.
    asset_ids : list[str] | None, default=None
        Asset IDs to generate. Defaults to two assets.

    Returns
    -------
    pl.DataFrame
        Generation DataFrame.

    """
    if asset_ids is None:
        asset_ids = ["asset_001", "asset_002"]

    spec = TimeRangeSpec(start=start, periods=periods, step=step)
    timestamps = _timestamps(spec)

    rows: list[dict[str, object]] = []
    for asset_index, asset_id in enumerate(asset_ids):
        capacity_mw = 100.0 + (asset_index * 10.0)
        for time_index, ts in enumerate(timestamps):
            # Deterministic relationship to support correlation tests later.
            net_mwh = 50.0 + asset_index + (time_index * 0.5)
            gross_mwh = net_mwh + 1.0
            curtailment = 0.0
            availability = 0.99
            rows.append(
                {
                    "timestamp": ts,
                    "asset_id": asset_id,
                    "gross_generation_mwh": float(gross_mwh),
                    "net_generation_mwh": float(net_mwh),
                    "curtailment_mwh": float(curtailment),
                    "availability_pct": float(availability),
                    "asset_capacity_mw": float(capacity_mw),
                }
            )

    return pl.DataFrame(rows).with_columns(
        [
            pl.col("timestamp").cast(pl.Datetime),
            pl.col("asset_id").cast(pl.Utf8),
        ]
    )

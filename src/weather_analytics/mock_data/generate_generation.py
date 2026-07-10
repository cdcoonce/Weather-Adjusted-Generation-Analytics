"""Generate mock generation data for the renewable + thermal fleet.

Thin wrapper over the physics-based fleet simulation
(:func:`weather_analytics.mock_data.simulate.simulate_fleet`). Produces hourly
records for every asset in :data:`weather_analytics.mock_data.fleet.FLEET` —
wind, solar, battery, and gas — carrying an explicit ``asset_type`` plus
technology-specific columns (battery SOC/throughput, gas fuel/heat-rate/CO2).

The Dagster ingestion asset merges these records into Snowflake RAW; dlt evolves
the RAW schema to add the new columns. Weather here is synthetic and seeded per
calendar day (shared ``weather_seed`` base), so ingestion is reproducible and
consistent with the weather ingestion asset by construction.
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from weather_analytics.mock_data.fleet import FLEET
from weather_analytics.mock_data.simulate import simulate_fleet

logger = logging.getLogger(__name__)

# Backward-compatible ``{asset_id: {"capacity_mw", "type"}}`` view of the fleet,
# derived from the single source of truth in ``fleet.FLEET``.
ASSET_CONFIGS: dict[str, dict[str, float | str]] = {
    asset.asset_id: {"capacity_mw": asset.capacity_mw, "type": asset.asset_type}
    for asset in FLEET
}


def generate_generation_data(
    start_date: str,
    end_date: str,
    asset_configs: dict[str, dict[str, float | str]] | None = None,  # noqa: ARG001
    random_seed: int = 42,
    warmup_days: int = 0,
) -> pl.DataFrame:
    """Generate realistic hourly generation for the full fleet.

    Parameters
    ----------
    start_date, end_date : str
        ISO-format datetimes (inclusive) at hourly resolution.
    asset_configs : dict | None
        Deprecated / ignored — the fleet is defined by ``fleet.FLEET``. Kept for
        backward compatibility with earlier callers.
    random_seed : int
        Seed for the (synthetic) weather and all stochastic physics.
    warmup_days : int
        Number of days to simulate before ``start_date`` to warm up stateful
        models (e.g., battery SOC). Output is filtered to [start_date, end_date].

    Returns
    -------
    pl.DataFrame
        Hourly generation with columns ``timestamp``, ``asset_id``,
        ``asset_type``, ``gross_generation_mwh``, ``net_generation_mwh``
        (negative for a charging battery), ``curtailment_mwh``,
        ``availability_pct``, ``asset_capacity_mw`` and the nullable
        technology-specific columns ``soc_pct``, ``charge_mwh``,
        ``discharge_mwh``, ``fuel_mmbtu``, ``heat_rate_btu_kwh``, ``co2_tonnes``.
    """
    logger.info(
        "Generating generation data from %s to %s for %d assets",
        start_date,
        end_date,
        len(FLEET),
    )
    result = simulate_fleet(
        start_date,
        end_date,
        FLEET,
        use_real_weather=False,
        random_seed=random_seed,
        warmup_days=warmup_days,
    )
    logger.info(
        "Generation data completed: %d rows, %d assets",
        result.generation.height,
        len(FLEET),
    )
    return result.generation


def save_generation_parquet(
    df: pl.DataFrame,
    output_dir: Path,
    partition_by_date: bool = True,
) -> Path:
    """Save generation data to Parquet files.

    Parameters
    ----------
    df : pl.DataFrame
        Generation DataFrame to persist.
    output_dir : Path
        Directory for output parquet files.
    partition_by_date : bool
        If True, write one file per date. Otherwise write a single file.

    Returns
    -------
    Path
        The output directory containing the written files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    if partition_by_date:
        dates = (
            df.select(pl.col("timestamp").dt.date().alias("date")).unique().sort("date")
        )
        logger.info("Saving %d daily generation files to %s", len(dates), output_dir)
        for (date_val,) in dates.iter_rows():
            date_str = date_val.isoformat()
            daily_df = df.filter(pl.col("timestamp").dt.date() == date_val)
            output_path = output_dir / f"generation_{date_str}.parquet"
            daily_df.write_parquet(output_path, compression="snappy")
        logger.info("Saved %d generation files", len(dates))
    else:
        output_path = output_dir / "generation_all.parquet"
        df.write_parquet(output_path, compression="snappy")
        logger.info("Saved generation data to %s", output_path)

    return output_dir


__all__ = [
    "ASSET_CONFIGS",
    "generate_generation_data",
    "save_generation_parquet",
]

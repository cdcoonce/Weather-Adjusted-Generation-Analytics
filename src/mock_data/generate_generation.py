"""Generate mock generation data for renewable energy assets."""

from datetime import datetime
from pathlib import Path

import numpy as np
import polars as pl

from src.config import config
from src.utils import get_logger

logger = get_logger(__name__)


# Asset configurations (capacity and type)
ASSET_CONFIGS = {
    "ASSET_001": {"capacity_mw": 50.0, "type": "wind"},
    "ASSET_002": {"capacity_mw": 75.0, "type": "wind"},
    "ASSET_003": {"capacity_mw": 100.0, "type": "wind"},
    "ASSET_004": {"capacity_mw": 45.0, "type": "wind"},
    "ASSET_005": {"capacity_mw": 80.0, "type": "wind"},
    "ASSET_006": {"capacity_mw": 30.0, "type": "solar"},
    "ASSET_007": {"capacity_mw": 50.0, "type": "solar"},
    "ASSET_008": {"capacity_mw": 40.0, "type": "solar"},
    "ASSET_009": {"capacity_mw": 60.0, "type": "solar"},
    "ASSET_010": {"capacity_mw": 35.0, "type": "solar"},
}


def wind_power_curve(wind_speed: np.ndarray, capacity_mw: float) -> np.ndarray:
    """
    Calculate wind turbine power output based on simplified power curve.

    Implements a typical wind turbine power curve with:
    - Cut-in speed: 3 m/s
    - Rated speed: 12 m/s
    - Cut-out speed: 25 m/s

    Parameters
    ----------
    wind_speed : np.ndarray
        Wind speed in m/s
    capacity_mw : float
        Asset capacity in MW

    Returns
    -------
    np.ndarray
        Power output in MW

    """
    power = np.zeros_like(wind_speed)

    # Cut-in to rated speed (3-12 m/s): cubic relationship
    mask_ramp = (wind_speed >= 3) & (wind_speed < 12)
    power[mask_ramp] = capacity_mw * ((wind_speed[mask_ramp] - 3) / 9) ** 3

    # Rated to cut-out speed (12-25 m/s): full power
    mask_rated = (wind_speed >= 12) & (wind_speed < 25)
    power[mask_rated] = capacity_mw

    # Above cut-out: no power
    power[wind_speed >= 25] = 0

    return power


def solar_power_output(ghi: np.ndarray, capacity_mw: float) -> np.ndarray:
    """
    Calculate solar PV power output based on irradiance.

    Assumes a simplified linear relationship with efficiency losses.

    Parameters
    ----------
    ghi : np.ndarray
        Global Horizontal Irradiance in W/m²
    capacity_mw : float
        Asset capacity in MW (at 1000 W/m² STC)

    Returns
    -------
    np.ndarray
        Power output in MW

    """
    # Efficiency factor (accounts for temperature, soiling, inverter losses)
    efficiency = 0.85

    # Linear relationship normalized to 1000 W/m²
    power = (ghi / 1000) * capacity_mw * efficiency

    return np.clip(power, 0, capacity_mw)


def generate_generation_data(
    start_date: str,
    end_date: str,
    asset_configs: dict[str, dict[str, float | str]],
    weather_df: pl.DataFrame | None = None,
    random_seed: int = 42,
) -> pl.DataFrame:
    """
    Generate realistic hourly generation data for renewable energy assets.

    Creates generation data correlated with weather conditions (if provided)
    or generates synthetic weather-correlated patterns. Includes:
    - Gross generation (theoretical output)
    - Net generation (after losses)
    - Curtailment
    - Availability

    Parameters
    ----------
    start_date : str
        Start date in YYYY-MM-DD format
    end_date : str
        End date in YYYY-MM-DD format
    asset_configs : dict
        Dictionary mapping asset_id to capacity_mw and type
    weather_df : pl.DataFrame, optional
        Weather dataframe to correlate generation with. If None, generates
        synthetic weather patterns.
    random_seed : int, default=42
        Random seed for reproducibility

    Returns
    -------
    pl.DataFrame
        DataFrame with columns: timestamp, asset_id, gross_generation_mwh,
        net_generation_mwh, curtailment_mwh, availability_pct, asset_capacity_mw

    Examples
    --------
    >>> generation_df = generate_generation_data(
    ...     start_date="2023-01-01",
    ...     end_date="2024-12-31",
    ...     asset_configs=ASSET_CONFIGS,
    ...     random_seed=42
    ... )

    """
    logger.info(
        f"Generating generation data from {start_date} to {end_date} "
        f"for {len(asset_configs)} assets"
    )

    np.random.seed(random_seed)

    # Generate hourly timestamps
    timestamps = pl.datetime_range(
        start=datetime.fromisoformat(start_date),
        end=datetime.fromisoformat(end_date),
        interval="1h",
        eager=True,
    )

    # Create base dataframe
    asset_ids = list(asset_configs.keys())
    base_df = pl.DataFrame({
        "timestamp": timestamps,
    }).join(
        pl.DataFrame({"asset_id": asset_ids}),
        how="cross",
    )

    n_rows = len(base_df)
    logger.info(
        f"Generated {n_rows:,} rows ({len(timestamps):,} hours × {len(asset_ids)} assets)"
    )

    # If weather data provided, join it
    if weather_df is not None:
        logger.info("Using provided weather data for generation correlation")
        df = base_df.join(
            weather_df,
            on=["timestamp", "asset_id"],
            how="left",
        )
    else:
        # Generate synthetic weather patterns
        logger.info("Generating synthetic weather patterns")
        df = base_df.with_columns([
            pl.col("timestamp").dt.hour().alias("hour"),
            pl.col("timestamp").dt.ordinal_day().alias("day_of_year"),
        ])

        hour_array = df["hour"].to_numpy()
        day_of_year = df["day_of_year"].to_numpy()

        # Synthetic wind speed
        seasonal_wind = 10 + 3 * np.sin(2 * np.pi * day_of_year / 365)
        diurnal_wind = 1.5 * np.sin(2 * np.pi * (hour_array - 6) / 24)
        wind_noise = np.random.normal(0, 2, n_rows)
        wind_speed = np.clip(seasonal_wind + diurnal_wind + wind_noise, 0, 25)

        # Synthetic GHI
        max_ghi = 900 + 100 * np.sin(2 * np.pi * (day_of_year - 80) / 365)
        solar_hour = hour_array - 12
        ghi_pattern = np.where(
            np.abs(solar_hour) < 6,
            max_ghi * (1 - (solar_hour / 8) ** 2),
            0
        )
        cloud_factor = np.random.uniform(0.7, 1.0, n_rows)
        ghi = np.clip(ghi_pattern * cloud_factor, 0, 1000)

        df = df.with_columns([
            pl.Series("wind_speed_mps", wind_speed),
            pl.Series("ghi", ghi),
        ])

    # Generate power output for each asset based on type
    gross_generation = np.zeros(n_rows)
    asset_capacity = np.zeros(n_rows)

    for idx, row in enumerate(df.select(["asset_id", "wind_speed_mps", "ghi"]).iter_rows()):
        asset_id_val, wind_speed_val, ghi_val = row

        config_info = asset_configs[asset_id_val]
        capacity = config_info["capacity_mw"]
        asset_type = config_info["type"]

        asset_capacity[idx] = capacity

        # Calculate gross generation based on asset type
        if asset_type == "wind":
            # Use wind power curve
            gross_generation[idx] = wind_power_curve(
                np.array([wind_speed_val if wind_speed_val else 0]),
                capacity
            )[0]
        else:  # solar
            # Use solar irradiance model
            gross_generation[idx] = solar_power_output(
                np.array([ghi_val if ghi_val else 0]),
                capacity
            )[0]

    # Add generation variability (equipment performance variation)
    performance_factor = np.random.uniform(0.95, 1.0, n_rows)
    gross_generation = gross_generation * performance_factor

    # Calculate losses (5-10% typical losses)
    loss_factor = np.random.uniform(0.90, 0.95, n_rows)
    net_generation = gross_generation * loss_factor

    # Calculate curtailment (0-15% of gross, more likely at high generation)
    curtailment_probability = gross_generation / (asset_capacity + 1e-6)
    curtailment_mask = np.random.random(n_rows) < (curtailment_probability * 0.1)
    curtailment = np.where(
        curtailment_mask,
        np.random.uniform(0.05, 0.15, n_rows) * gross_generation,
        0
    )
    # Adjust net generation for curtailment
    net_generation = np.maximum(net_generation - curtailment, 0)

    # Calculate availability (85-100%)
    base_availability = 95
    availability_variation = np.random.normal(0, 5, n_rows)
    availability = np.clip(base_availability + availability_variation, 85, 100)

    # Apply availability to generation
    availability_factor = availability / 100
    gross_generation = gross_generation * availability_factor
    net_generation = net_generation * availability_factor

    # Create final dataframe
    result = df.select([
        "timestamp",
        "asset_id",
    ]).with_columns([
        pl.Series("gross_generation_mwh", gross_generation),
        pl.Series("net_generation_mwh", net_generation),
        pl.Series("curtailment_mwh", curtailment),
        pl.Series("availability_pct", availability),
        pl.Series("asset_capacity_mw", asset_capacity),
    ])

    # Sort by timestamp and asset_id
    result = result.sort(["timestamp", "asset_id"])

    logger.info(
        "Generation data generation completed",
        extra={
            "extra_fields": {
                "rows": len(result),
                "assets": len(asset_configs),
                "date_range": f"{start_date} to {end_date}",
                "total_capacity_mw": sum(c["capacity_mw"] for c in asset_configs.values()),
            }
        },
    )

    return result


def save_generation_parquet(
    df: pl.DataFrame,
    output_dir: Path,
    partition_by_date: bool = True,
) -> None:
    """
    Save generation data to Parquet files.

    Optionally partitions by date to create daily files for incremental
    loading.

    Parameters
    ----------
    df : pl.DataFrame
        Generation dataframe to save
    output_dir : Path
        Output directory for Parquet files
    partition_by_date : bool, default=True
        If True, creates separate files for each date

    Returns
    -------
    None

    """
    output_dir.mkdir(parents=True, exist_ok=True)

    if partition_by_date:
        # Get unique dates
        dates = df.select(
            pl.col("timestamp").dt.date().alias("date")
        ).unique().sort("date")

        logger.info(f"Saving {len(dates)} daily generation files to {output_dir}")

        for date_row in dates.iter_rows():
            date_val = date_row[0]
            date_str = date_val.isoformat()

            # Filter data for this date
            daily_df = df.filter(
                pl.col("timestamp").dt.date() == date_val
            )

            # Save to parquet
            output_path = output_dir / f"generation_{date_str}.parquet"
            daily_df.write_parquet(output_path, compression="snappy")

        logger.info(f"Saved {len(dates)} generation files")
    else:
        # Save as single file
        output_path = output_dir / "generation_all.parquet"
        df.write_parquet(output_path, compression="snappy")
        logger.info(f"Saved generation data to {output_path}")


def main() -> None:
    """
    Main entry point for generation data generation.

    Reads configuration from environment and generates mock generation data.

    Returns
    -------
    None

    """
    logger.info("Starting generation data generation")

    # Ensure directories exist
    config.ensure_directories()

    # Load weather data if available (for correlation)
    weather_df = None
    try:
        weather_files = list(config.weather_raw_path.glob("weather_*.parquet"))
        if weather_files:
            logger.info(f"Loading {len(weather_files)} weather files for correlation")
            weather_df = pl.concat([
                pl.read_parquet(f) for f in weather_files
            ])
            logger.info(f"Loaded weather data: {weather_df.shape}")
    except Exception as e:
        logger.warning(f"Could not load weather data: {e}. Using synthetic patterns.")

    # Generate generation data
    generation_df = generate_generation_data(
        start_date=config.mock_start_date,
        end_date=config.mock_end_date,
        asset_configs=ASSET_CONFIGS,
        weather_df=weather_df,
        random_seed=config.mock_random_seed,
    )

    # Display sample
    logger.info("Sample generation data:")
    print(generation_df.head(10))
    print("\nDataFrame shape:", generation_df.shape)
    print("\nDataFrame schema:")
    print(generation_df.schema)

    # Calculate and display summary statistics
    summary = generation_df.group_by("asset_id").agg([
        pl.col("gross_generation_mwh").mean().alias("avg_gross_mwh"),
        pl.col("net_generation_mwh").mean().alias("avg_net_mwh"),
        pl.col("curtailment_mwh").mean().alias("avg_curtailment_mwh"),
        pl.col("availability_pct").mean().alias("avg_availability_pct"),
        pl.col("asset_capacity_mw").first().alias("capacity_mw"),
    ])
    print("\nAsset summary:")
    print(summary)

    # Save to Parquet
    save_generation_parquet(
        df=generation_df,
        output_dir=config.generation_raw_path,
        partition_by_date=True,
    )

    logger.info("Generation data generation completed successfully")


if __name__ == "__main__":
    main()

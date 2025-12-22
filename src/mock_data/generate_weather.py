"""Generate mock weather data for renewable energy assets."""

from datetime import datetime
from pathlib import Path

import numpy as np
import polars as pl

from src.config import config
from src.utils import get_logger

logger = get_logger(__name__)


def generate_weather_data(
    start_date: str,
    end_date: str,
    asset_count: int,
    random_seed: int = 42,
) -> pl.DataFrame:
    """
    Generate realistic hourly weather data for multiple renewable energy assets.

    Creates time-series weather data with realistic patterns including:
    - Diurnal temperature variation
    - Seasonal GHI patterns
    - Correlated wind speed and direction
    - Atmospheric pressure variation
    - Relative humidity patterns

    Parameters
    ----------
    start_date : str
        Start date in YYYY-MM-DD format
    end_date : str
        End date in YYYY-MM-DD format
    asset_count : int
        Number of assets to generate data for
    random_seed : int, default=42
        Random seed for reproducibility

    Returns
    -------
    pl.DataFrame
        DataFrame with columns: timestamp, asset_id, wind_speed_mps,
        wind_direction_deg, ghi, temperature_c, pressure_hpa, relative_humidity

    Examples
    --------
    >>> weather_df = generate_weather_data(
    ...     start_date="2023-01-01",
    ...     end_date="2024-12-31",
    ...     asset_count=10,
    ...     random_seed=42
    ... )

    """
    logger.info(
        f"Generating weather data from {start_date} to {end_date} "
        f"for {asset_count} assets"
    )

    # Set random seed for reproducibility
    np.random.seed(random_seed)

    # Generate hourly timestamps
    timestamps = pl.datetime_range(
        start=datetime.fromisoformat(start_date),
        end=datetime.fromisoformat(end_date),
        interval="1h",
        eager=True,
    )

    # Generate asset IDs
    asset_ids = [f"ASSET_{str(i+1).zfill(3)}" for i in range(asset_count)]

    # Create base dataframe with all combinations
    base_df = pl.DataFrame({
        "timestamp": timestamps,
    }).join(
        pl.DataFrame({"asset_id": asset_ids}),
        how="cross",
    )

    n_rows = len(base_df)
    logger.info(f"Generated {n_rows:,} rows ({len(timestamps):,} hours × {asset_count} assets)")

    # Extract time features for pattern generation
    df = base_df.with_columns([
        pl.col("timestamp").dt.hour().alias("hour"),
        pl.col("timestamp").dt.month().alias("month"),
        pl.col("timestamp").dt.ordinal_day().alias("day_of_year"),
    ])

    # Generate wind speed (0-25 m/s with diurnal and seasonal patterns)
    # Base wind speed with seasonal variation
    seasonal_wind = 10 + 3 * np.sin(2 * np.pi * df["day_of_year"].to_numpy() / 365)
    # Diurnal pattern (higher during day)
    diurnal_wind = 1.5 * np.sin(2 * np.pi * (df["hour"].to_numpy() - 6) / 24)
    # Random variation
    wind_noise = np.random.normal(0, 2, n_rows)
    wind_speed = np.clip(seasonal_wind + diurnal_wind + wind_noise, 0, 25)

    # Generate wind direction (0-360 degrees with some persistence)
    # Base direction varies by asset (simulating different locations)
    asset_base_direction = {
        asset: np.random.uniform(0, 360) for asset in asset_ids
    }
    base_direction = np.array([
        asset_base_direction[asset] for asset in df["asset_id"].to_list()
    ])
    # Add random walk for temporal variation
    direction_change = np.random.normal(0, 20, n_rows)
    wind_direction = (base_direction + direction_change) % 360

    # Generate GHI (Global Horizontal Irradiance, 0-1000 W/m²)
    # Only during daylight hours with seasonal variation
    hour_array = df["hour"].to_numpy()
    day_of_year = df["day_of_year"].to_numpy()

    # Solar noon irradiance with seasonal variation
    max_ghi = 900 + 100 * np.sin(2 * np.pi * (day_of_year - 80) / 365)

    # Diurnal pattern (parabolic, zero at night)
    solar_hour = hour_array - 12  # Hours from solar noon
    ghi_pattern = np.where(
        np.abs(solar_hour) < 6,
        max_ghi * (1 - (solar_hour / 8) ** 2),
        0
    )

    # Add cloud cover variation
    cloud_factor = np.random.uniform(0.7, 1.0, n_rows)
    ghi = np.clip(ghi_pattern * cloud_factor, 0, 1000)

    # Generate temperature (-10 to 35°C with diurnal and seasonal patterns)
    # Seasonal base temperature
    seasonal_temp = 15 + 12 * np.sin(2 * np.pi * (day_of_year - 80) / 365)
    # Diurnal variation (warmer in afternoon)
    diurnal_temp = 5 * np.sin(2 * np.pi * (hour_array - 6) / 24)
    # Random variation
    temp_noise = np.random.normal(0, 2, n_rows)
    temperature = seasonal_temp + diurnal_temp + temp_noise

    # Generate atmospheric pressure (980-1030 hPa)
    # Varies with season and weather systems
    base_pressure = 1013
    seasonal_pressure = 5 * np.sin(2 * np.pi * day_of_year / 365)
    pressure_noise = np.random.normal(0, 5, n_rows)
    pressure = base_pressure + seasonal_pressure + pressure_noise

    # Generate relative humidity (20-95%)
    # Inversely correlated with temperature and GHI
    base_humidity = 65
    temp_effect = -0.5 * (temperature - 15)
    ghi_effect = -0.01 * ghi
    humidity_noise = np.random.normal(0, 10, n_rows)
    relative_humidity = np.clip(
        base_humidity + temp_effect + ghi_effect + humidity_noise,
        20,
        95
    )

    # Create final dataframe
    result = df.select([
        "timestamp",
        "asset_id",
    ]).with_columns([
        pl.Series("wind_speed_mps", wind_speed),
        pl.Series("wind_direction_deg", wind_direction),
        pl.Series("ghi", ghi),
        pl.Series("temperature_c", temperature),
        pl.Series("pressure_hpa", pressure),
        pl.Series("relative_humidity", relative_humidity),
    ])

    # Sort by timestamp and asset_id
    result = result.sort(["timestamp", "asset_id"])

    logger.info(
        "Weather data generation completed",
        extra={
            "extra_fields": {
                "rows": len(result),
                "assets": asset_count,
                "date_range": f"{start_date} to {end_date}",
            }
        },
    )

    return result


def save_weather_parquet(
    df: pl.DataFrame,
    output_dir: Path,
    partition_by_date: bool = True,
) -> None:
    """
    Save weather data to Parquet files.

    Optionally partitions by date to create daily files for incremental
    loading.

    Parameters
    ----------
    df : pl.DataFrame
        Weather dataframe to save
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

        logger.info(f"Saving {len(dates)} daily weather files to {output_dir}")

        for date_row in dates.iter_rows():
            date_val = date_row[0]
            date_str = date_val.isoformat()

            # Filter data for this date
            daily_df = df.filter(
                pl.col("timestamp").dt.date() == date_val
            )

            # Save to parquet
            output_path = output_dir / f"weather_{date_str}.parquet"
            daily_df.write_parquet(output_path, compression="snappy")

        logger.info(f"Saved {len(dates)} weather files")
    else:
        # Save as single file
        output_path = output_dir / "weather_all.parquet"
        df.write_parquet(output_path, compression="snappy")
        logger.info(f"Saved weather data to {output_path}")


def main() -> None:
    """
    Main entry point for weather data generation.

    Reads configuration from environment and generates mock weather data.

    Returns
    -------
    None

    """
    logger.info("Starting weather data generation")

    # Ensure directories exist
    config.ensure_directories()

    # Generate weather data
    weather_df = generate_weather_data(
        start_date=config.mock_start_date,
        end_date=config.mock_end_date,
        asset_count=config.mock_asset_count,
        random_seed=config.mock_random_seed,
    )

    # Display sample
    logger.info("Sample weather data:")
    print(weather_df.head(10))
    print("\nDataFrame shape:", weather_df.shape)
    print("\nDataFrame schema:")
    print(weather_df.schema)

    # Save to Parquet
    save_weather_parquet(
        df=weather_df,
        output_dir=config.weather_raw_path,
        partition_by_date=True,
    )

    logger.info("Weather data generation completed successfully")


if __name__ == "__main__":
    main()

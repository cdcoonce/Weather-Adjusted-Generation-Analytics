"""Generate mock weather data for renewable energy assets."""

from datetime import datetime
from pathlib import Path

import numpy as np
import polars as pl

from weather_adjusted_generation_analytics.config import config
from weather_adjusted_generation_analytics.utils import get_logger

logger = get_logger(__name__)


def generate_weather_data(
	start_date: str,
	end_date: str,
	asset_count: int,
	random_seed: int = 42,
) -> pl.DataFrame:
	"""Generate realistic hourly weather data for multiple assets."""
	logger.info(
		f"Generating weather data from {start_date} to {end_date} "
		f"for {asset_count} assets"
	)

	np.random.seed(random_seed)

	timestamps = pl.datetime_range(
		start=datetime.fromisoformat(start_date),
		end=datetime.fromisoformat(end_date),
		interval="1h",
		eager=True,
	)

	asset_ids = [f"ASSET_{str(i + 1).zfill(3)}" for i in range(asset_count)]

	base_df = pl.DataFrame({"timestamp": timestamps}).join(
		pl.DataFrame({"asset_id": asset_ids}),
		how="cross",
	)

	n_rows = len(base_df)
	logger.info(
		f"Generated {n_rows:,} rows ({len(timestamps):,} hours × {asset_count} assets)"
	)

	df = base_df.with_columns(
		[
			pl.col("timestamp").dt.hour().alias("hour"),
			pl.col("timestamp").dt.month().alias("month"),
			pl.col("timestamp").dt.ordinal_day().alias("day_of_year"),
		]
	)

	seasonal_wind = 10 + 3 * np.sin(2 * np.pi * df["day_of_year"].to_numpy() / 365)
	diurnal_wind = 1.5 * np.sin(2 * np.pi * (df["hour"].to_numpy() - 6) / 24)
	wind_noise = np.random.normal(0, 2, n_rows)
	wind_speed = np.clip(seasonal_wind + diurnal_wind + wind_noise, 0, 25)

	asset_base_direction = {asset: np.random.uniform(0, 360) for asset in asset_ids}
	base_direction = np.array([asset_base_direction[a] for a in df["asset_id"].to_list()])
	direction_change = np.random.normal(0, 20, n_rows)
	wind_direction = (base_direction + direction_change) % 360

	hour_array = df["hour"].to_numpy()
	day_of_year = df["day_of_year"].to_numpy()

	max_ghi = 900 + 100 * np.sin(2 * np.pi * (day_of_year - 80) / 365)
	solar_hour = hour_array - 12
	ghi_pattern = np.where(
		np.abs(solar_hour) < 6,
		max_ghi * (1 - (solar_hour / 8) ** 2),
		0,
	)

	cloud_factor = np.random.uniform(0.7, 1.0, n_rows)
	ghi = np.clip(ghi_pattern * cloud_factor, 0, 1000)

	seasonal_temp = 15 + 12 * np.sin(2 * np.pi * (day_of_year - 80) / 365)
	diurnal_temp = 5 * np.sin(2 * np.pi * (hour_array - 6) / 24)
	temp_noise = np.random.normal(0, 2, n_rows)
	temperature = seasonal_temp + diurnal_temp + temp_noise

	base_pressure = 1013
	seasonal_pressure = 5 * np.sin(2 * np.pi * day_of_year / 365)
	pressure_noise = np.random.normal(0, 5, n_rows)
	pressure = base_pressure + seasonal_pressure + pressure_noise

	base_humidity = 65
	temp_effect = -0.5 * (temperature - 15)
	ghi_effect = -0.01 * ghi
	humidity_noise = np.random.normal(0, 10, n_rows)
	relative_humidity = np.clip(
		base_humidity + temp_effect + ghi_effect + humidity_noise,
		20,
		95,
	)

	result = (
		df.select(["timestamp", "asset_id"])
		.with_columns(
			[
				pl.Series("wind_speed_mps", wind_speed),
				pl.Series("wind_direction_deg", wind_direction),
				pl.Series("ghi", ghi),
				pl.Series("temperature_c", temperature),
				pl.Series("pressure_hpa", pressure),
				pl.Series("relative_humidity", relative_humidity),
			]
		)
		.sort(["timestamp", "asset_id"])
	)

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
	"""Save weather data to Parquet files (optionally daily-partitioned)."""
	output_dir.mkdir(parents=True, exist_ok=True)

	if partition_by_date:
		dates = (
			df.select(pl.col("timestamp").dt.date().alias("date"))
			.unique()
			.sort("date")
		)
		logger.info(f"Saving {len(dates)} daily weather files to {output_dir}")

		for (date_val,) in dates.iter_rows():
			date_str = date_val.isoformat()
			daily_df = df.filter(pl.col("timestamp").dt.date() == date_val)
			output_path = output_dir / f"weather_{date_str}.parquet"
			daily_df.write_parquet(output_path, compression="snappy")

		logger.info(f"Saved {len(dates)} weather files")
		return

	output_path = output_dir / "weather_all.parquet"
	df.write_parquet(output_path, compression="snappy")
	logger.info(f"Saved weather data to {output_path}")


def main() -> None:
	"""Main entry point for weather data generation."""
	logger.info("Starting weather data generation")
	config.ensure_directories()

	weather_df = generate_weather_data(
		start_date=config.mock_start_date,
		end_date=config.mock_end_date,
		asset_count=config.mock_asset_count,
		random_seed=config.mock_random_seed,
	)

	logger.info("Sample weather data:")
	print(weather_df.head(10))
	print("\nDataFrame shape:", weather_df.shape)
	print("\nDataFrame schema:")
	print(weather_df.schema)

	save_weather_parquet(
		df=weather_df,
		output_dir=config.weather_raw_path,
		partition_by_date=True,
	)

	logger.info("Weather data generation completed successfully")


if __name__ == "__main__":
	main()


__all__ = ["generate_weather_data", "save_weather_parquet"]


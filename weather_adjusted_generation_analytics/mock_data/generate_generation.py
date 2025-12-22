"""Generate mock generation data for renewable energy assets."""

from datetime import datetime
from pathlib import Path

import numpy as np
import polars as pl

from weather_adjusted_generation_analytics.config import config
from weather_adjusted_generation_analytics.utils import get_logger

logger = get_logger(__name__)


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
	"""Calculate wind turbine power output based on simplified power curve."""
	power = np.zeros_like(wind_speed)

	mask_ramp = (wind_speed >= 3) & (wind_speed < 12)
	power[mask_ramp] = capacity_mw * ((wind_speed[mask_ramp] - 3) / 9) ** 3

	mask_rated = (wind_speed >= 12) & (wind_speed < 25)
	power[mask_rated] = capacity_mw

	power[wind_speed >= 25] = 0
	return power


def solar_power_output(ghi: np.ndarray, capacity_mw: float) -> np.ndarray:
	"""Calculate solar PV power output based on irradiance."""
	efficiency = 0.85
	power = (ghi / 1000) * capacity_mw * efficiency
	return np.clip(power, 0, capacity_mw)


def generate_generation_data(
	start_date: str,
	end_date: str,
	asset_configs: dict[str, dict[str, float | str]],
	weather_df: pl.DataFrame | None = None,
	random_seed: int = 42,
) -> pl.DataFrame:
	"""Generate realistic hourly generation data for renewable energy assets."""
	logger.info(
		f"Generating generation data from {start_date} to {end_date} "
		f"for {len(asset_configs)} assets"
	)

	np.random.seed(random_seed)

	timestamps = pl.datetime_range(
		start=datetime.fromisoformat(start_date),
		end=datetime.fromisoformat(end_date),
		interval="1h",
		eager=True,
	)

	asset_ids = list(asset_configs.keys())
	base_df = pl.DataFrame({"timestamp": timestamps}).join(
		pl.DataFrame({"asset_id": asset_ids}),
		how="cross",
	)

	n_rows = len(base_df)
	logger.info(
		f"Generated {n_rows:,} rows ({len(timestamps):,} hours × {len(asset_ids)} assets)"
	)

	if weather_df is not None:
		logger.info("Using provided weather data for generation correlation")
		df = base_df.join(weather_df, on=["timestamp", "asset_id"], how="left")
	else:
		logger.info("Generating synthetic weather patterns")
		df = base_df.with_columns(
			[
				pl.col("timestamp").dt.hour().alias("hour"),
				pl.col("timestamp").dt.ordinal_day().alias("day_of_year"),
			]
		)

		hour_array = df["hour"].to_numpy()
		day_of_year = df["day_of_year"].to_numpy()

		seasonal_wind = 10 + 3 * np.sin(2 * np.pi * day_of_year / 365)
		diurnal_wind = 1.5 * np.sin(2 * np.pi * (hour_array - 6) / 24)
		wind_noise = np.random.normal(0, 2, n_rows)
		wind_speed = np.clip(seasonal_wind + diurnal_wind + wind_noise, 0, 25)

		max_ghi = 900 + 100 * np.sin(2 * np.pi * (day_of_year - 80) / 365)
		solar_hour = hour_array - 12
		ghi_pattern = np.where(
			np.abs(solar_hour) < 6,
			max_ghi * (1 - (solar_hour / 8) ** 2),
			0,
		)
		cloud_factor = np.random.uniform(0.7, 1.0, n_rows)
		ghi = np.clip(ghi_pattern * cloud_factor, 0, 1000)

		df = df.with_columns(
			[
				pl.Series("wind_speed_mps", wind_speed),
				pl.Series("ghi", ghi),
			]
		)

	gross_generation = np.zeros(n_rows)
	asset_capacity = np.zeros(n_rows)

	for idx, row in enumerate(
		df.select(["asset_id", "wind_speed_mps", "ghi"]).iter_rows()
	):
		asset_id_val, wind_speed_val, ghi_val = row
		config_info = asset_configs[asset_id_val]
		capacity = config_info["capacity_mw"]
		asset_type = config_info["type"]

		asset_capacity[idx] = float(capacity)

		if asset_type == "wind":
			gross_generation[idx] = wind_power_curve(
				np.array([wind_speed_val if wind_speed_val else 0]),
				float(capacity),
			)[0]
		else:
			gross_generation[idx] = solar_power_output(
				np.array([ghi_val if ghi_val else 0]),
				float(capacity),
			)[0]

	performance_factor = np.random.uniform(0.95, 1.0, n_rows)
	gross_generation = gross_generation * performance_factor

	loss_factor = np.random.uniform(0.90, 0.95, n_rows)
	net_generation = gross_generation * loss_factor

	curtailment_probability = gross_generation / (asset_capacity + 1e-6)
	curtailment_mask = np.random.random(n_rows) < (curtailment_probability * 0.1)
	curtailment = np.where(
		curtailment_mask,
		np.random.uniform(0.05, 0.15, n_rows) * gross_generation,
		0,
	)
	net_generation = np.maximum(net_generation - curtailment, 0)

	base_availability = 95
	availability_variation = np.random.normal(0, 5, n_rows)
	availability = np.clip(base_availability + availability_variation, 85, 100)

	availability_factor = availability / 100
	gross_generation = gross_generation * availability_factor
	net_generation = net_generation * availability_factor

	result = (
		df.select(["timestamp", "asset_id"])
		.with_columns(
			[
				pl.Series("gross_generation_mwh", gross_generation),
				pl.Series("net_generation_mwh", net_generation),
				pl.Series("curtailment_mwh", curtailment),
				pl.Series("availability_pct", availability),
				pl.Series("asset_capacity_mw", asset_capacity),
			]
		)
		.sort(["timestamp", "asset_id"])
	)

	logger.info(
		"Generation data generation completed",
		extra={
			"extra_fields": {
				"rows": len(result),
				"assets": len(asset_configs),
				"date_range": f"{start_date} to {end_date}",
				"total_capacity_mw": sum(
					float(c["capacity_mw"]) for c in asset_configs.values()
				),
			}
		},
	)
	return result


def save_generation_parquet(
	df: pl.DataFrame,
	output_dir: Path,
	partition_by_date: bool = True,
) -> None:
	"""Save generation data to Parquet files (optionally daily-partitioned)."""
	output_dir.mkdir(parents=True, exist_ok=True)

	if partition_by_date:
		dates = (
			df.select(pl.col("timestamp").dt.date().alias("date"))
			.unique()
			.sort("date")
		)

		logger.info(f"Saving {len(dates)} daily generation files to {output_dir}")

		for (date_val,) in dates.iter_rows():
			date_str = date_val.isoformat()
			daily_df = df.filter(pl.col("timestamp").dt.date() == date_val)
			output_path = output_dir / f"generation_{date_str}.parquet"
			daily_df.write_parquet(output_path, compression="snappy")

		logger.info(f"Saved {len(dates)} generation files")
		return

	output_path = output_dir / "generation_all.parquet"
	df.write_parquet(output_path, compression="snappy")
	logger.info(f"Saved generation data to {output_path}")


def main() -> None:
	"""Main entry point for generation data generation."""
	logger.info("Starting generation data generation")
	config.ensure_directories()

	generation_df = generate_generation_data(
		start_date=config.mock_start_date,
		end_date=config.mock_end_date,
		asset_configs=ASSET_CONFIGS,
		random_seed=config.mock_random_seed,
	)

	logger.info("Sample generation data:")
	print(generation_df.head(10))
	print("\nDataFrame shape:", generation_df.shape)
	print("\nDataFrame schema:")
	print(generation_df.schema)

	save_generation_parquet(
		df=generation_df,
		output_dir=config.generation_raw_path,
		partition_by_date=True,
	)

	logger.info("Generation data generation completed successfully")


if __name__ == "__main__":
	main()


__all__ = [
	"ASSET_CONFIGS",
	"generate_generation_data",
	"save_generation_parquet",
	"solar_power_output",
	"wind_power_curve",
]


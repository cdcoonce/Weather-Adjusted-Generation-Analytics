"""Configuration management using Pydantic and environment variables."""

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    """
    Central configuration for the renewable performance pipeline.

    Loads configuration from environment variables and .env files.
    All paths are converted to absolute Path objects for consistency.

    Attributes
    ----------
    data_raw : Path
        Root directory for raw data files (Parquet)
    data_intermediate : Path
        Directory for intermediate processing outputs
    data_processed : Path
        Directory for final processed outputs
    duckdb_path : Path
        Path to the DuckDB database file
    duckdb_memory_limit : str
        Memory limit for DuckDB (e.g., '8GB')
    duckdb_threads : int
        Number of threads for DuckDB queries
    dlt_schema : str
        Schema name for dlt pipeline
    dlt_pipeline_name : str
        Pipeline name for dlt
    dlt_destination : str
        Destination type for dlt (duckdb)
    mock_start_date : str
        Start date for mock data generation (YYYY-MM-DD)
    mock_end_date : str
        End date for mock data generation (YYYY-MM-DD)
    mock_asset_count : int
        Number of assets to generate in mock data
    mock_random_seed : int
        Random seed for reproducible mock data
    dbt_profiles_dir : Path
        Directory containing dbt profiles
    dbt_project_dir : Path
        Root directory of dbt project
    dagster_home : Path
        Dagster home directory
    log_level : str
        Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    log_format : Literal["json", "text"]
        Log output format

    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Data directories
    data_raw: Path = Field(
        default=Path("data/raw"),
        description="Root directory for raw data files",
    )
    data_intermediate: Path = Field(
        default=Path("data/intermediate"),
        description="Directory for intermediate processing",
    )
    data_processed: Path = Field(
        default=Path("data/processed"),
        description="Directory for final processed outputs",
    )

    # DuckDB configuration
    duckdb_path: Path = Field(
        default=Path("data/warehouse.duckdb"),
        description="Path to DuckDB database file",
    )
    duckdb_memory_limit: str = Field(
        default="8GB",
        description="Memory limit for DuckDB",
    )
    duckdb_threads: int = Field(
        default=4,
        description="Number of threads for DuckDB",
    )

    # dlt configuration
    dlt_schema: str = Field(
        default="renewable_energy",
        description="Schema name for dlt pipeline",
    )
    dlt_pipeline_name: str = Field(
        default="renewable_ingestion",
        description="Pipeline name for dlt",
    )
    dlt_destination: str = Field(
        default="duckdb",
        description="Destination type for dlt",
    )

    # Mock data configuration
    mock_start_date: str = Field(
        default="2023-01-01",
        description="Start date for mock data (YYYY-MM-DD)",
    )
    mock_end_date: str = Field(
        default="2024-12-31",
        description="End date for mock data (YYYY-MM-DD)",
    )
    mock_asset_count: int = Field(
        default=10,
        description="Number of assets to generate",
        ge=1,
        le=100,
    )
    mock_random_seed: int = Field(
        default=42,
        description="Random seed for reproducibility",
    )

    # dbt configuration
    dbt_profiles_dir: Path = Field(
        default=Path("dbt/renewable_dbt/profiles"),
        description="dbt profiles directory",
    )
    dbt_project_dir: Path = Field(
        default=Path("dbt/renewable_dbt"),
        description="dbt project directory",
    )

    # Dagster configuration
    dagster_home: Path = Field(
        default=Path("dags/dagster_project"),
        description="Dagster home directory",
    )

    # Logging configuration
    log_level: str = Field(
        default="INFO",
        description="Logging level",
    )
    log_format: Literal["json", "text"] = Field(
        default="json",
        description="Log output format",
    )

    @property
    def weather_raw_path(self) -> Path:
        """Path to raw weather data directory."""
        return self.data_raw / "weather"

    @property
    def generation_raw_path(self) -> Path:
        """Path to raw generation data directory."""
        return self.data_raw / "generation"

    def ensure_directories(self) -> None:
        """
        Create all required directories if they don't exist.

        This method should be called during initialization to ensure
        all necessary directories are present before pipeline execution.

        Returns
        -------
        None

        """
        directories = [
            self.data_raw,
            self.weather_raw_path,
            self.generation_raw_path,
            self.data_intermediate,
            self.data_processed,
            self.dbt_profiles_dir,
            self.dagster_home,
        ]

        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)


# Global configuration instance
config = Config()

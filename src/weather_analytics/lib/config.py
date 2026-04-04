"""Centralized configuration for WAGA pipeline via environment variables."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class WAGAConfig(BaseSettings):
    """Configuration for the Weather Adjusted Generation Analytics pipeline.

    All settings are loaded from environment variables with the ``WAGA_``
    prefix, or from a ``.env`` file in the project root.

    Parameters
    ----------
    snowflake_account : str
        Snowflake account identifier.
    snowflake_user : str
        Snowflake service account username.
    snowflake_private_key_base64 : str
        Base64-encoded PEM private key.
    snowflake_private_key_path : str
        Path to PEM private key file (for dbt).
    snowflake_warehouse : str
        Snowflake compute warehouse.
    snowflake_database : str
        Snowflake database name.
    snowflake_role : str
        Snowflake role.
    dlt_pipeline_name : str
        dlt pipeline name.
    dlt_dataset_name : str
        dlt dataset/schema name.
    mock_start_date : str
        Mock data start date.
    mock_end_date : str
        Mock data end date.
    mock_asset_count : int
        Number of mock assets to generate.
    mock_random_seed : int
        Random seed for reproducibility.
    log_level : str
        Logging level.
    log_format : str
        Log format (json or text).
    """

    model_config = SettingsConfigDict(
        env_prefix="WAGA_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Snowflake
    snowflake_account: str = Field(
        description="Snowflake account identifier",
    )
    snowflake_user: str = Field(
        description="Snowflake service account username",
    )
    snowflake_private_key_base64: str = Field(
        description="Base64-encoded PEM private key",
    )
    snowflake_private_key_path: str = Field(
        default="",
        description="Path to PEM private key file (for dbt)",
    )
    snowflake_warehouse: str = Field(
        description="Snowflake compute warehouse",
    )
    snowflake_database: str = Field(
        default="WAGA",
        description="Snowflake database",
    )
    snowflake_role: str = Field(
        description="Snowflake role",
    )

    # dlt
    dlt_pipeline_name: str = Field(
        default="waga_ingestion",
        description="dlt pipeline name",
    )
    dlt_dataset_name: str = Field(
        default="RAW",
        description="dlt dataset/schema name",
    )

    # Mock data
    mock_start_date: str = Field(
        default="2023-01-01",
        description="Mock data start date",
    )
    mock_end_date: str = Field(
        default="2024-12-31",
        description="Mock data end date",
    )
    mock_asset_count: int = Field(
        default=10,
        ge=1,
        le=100,
        description="Number of mock assets",
    )
    mock_random_seed: int = Field(
        default=42,
        description="Random seed for reproducibility",
    )

    # Logging
    log_level: str = Field(
        default="INFO",
        description="Logging level",
    )
    log_format: str = Field(
        default="json",
        description="Log format (json or text)",
    )

"""Unit tests for WAGAConfig (src/weather_analytics/lib/config.py)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from weather_analytics.lib.config import WAGAConfig


def _config_no_dotenv(**overrides: str) -> WAGAConfig:
    """Construct WAGAConfig without reading .env file."""
    return WAGAConfig(_env_file=None, **overrides)  # type: ignore[call-arg]


@pytest.mark.unit
class TestWAGAConfigDefaults:
    """Verify default field values when required env vars are supplied."""

    @pytest.fixture
    def _required_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Set the minimum required WAGA_ environment variables."""
        monkeypatch.setenv("WAGA_SNOWFLAKE_ACCOUNT", "xy12345.us-east-1")
        monkeypatch.setenv("WAGA_SNOWFLAKE_USER", "svc_waga")
        monkeypatch.setenv("WAGA_SNOWFLAKE_PRIVATE_KEY_BASE64", "dGVzdA==")
        monkeypatch.setenv("WAGA_SNOWFLAKE_WAREHOUSE", "WAGA_WH")
        monkeypatch.setenv("WAGA_SNOWFLAKE_ROLE", "WAGA_ROLE")
        # Clear any .env-sourced values that would override defaults
        monkeypatch.delenv("WAGA_SNOWFLAKE_PRIVATE_KEY_PATH", raising=False)
        monkeypatch.delenv("WAGA_SNOWFLAKE_DATABASE", raising=False)
        monkeypatch.delenv("WAGA_DLT_PIPELINE_NAME", raising=False)
        monkeypatch.delenv("WAGA_DLT_DATASET_NAME", raising=False)

    @pytest.mark.usefixtures("_required_env")
    def test_loads_with_env_vars(self) -> None:
        cfg = _config_no_dotenv()
        assert cfg.snowflake_account == "xy12345.us-east-1"
        assert cfg.snowflake_user == "svc_waga"

    @pytest.mark.usefixtures("_required_env")
    def test_default_values(self) -> None:
        expected_asset_count = 10
        expected_random_seed = 42
        cfg = _config_no_dotenv()
        assert cfg.snowflake_database == "WAGA"
        assert cfg.snowflake_private_key_path == ""
        assert cfg.dlt_pipeline_name == "waga_ingestion"
        assert cfg.dlt_dataset_name == "RAW"
        assert cfg.mock_start_date == "2023-01-01"
        assert cfg.mock_end_date == "2024-12-31"
        assert cfg.mock_asset_count == expected_asset_count
        assert cfg.mock_random_seed == expected_random_seed
        assert cfg.log_level == "INFO"
        assert cfg.log_format == "json"

    @pytest.mark.usefixtures("_required_env")
    def test_env_prefix_is_waga(self) -> None:
        assert WAGAConfig.model_config["env_prefix"] == "WAGA_"


@pytest.mark.unit
class TestWAGAConfigValidation:
    """Validation edge cases."""

    def test_missing_required_fields_raises(self) -> None:
        with pytest.raises(ValidationError):
            _config_no_dotenv(snowflake_account="x")

    def test_mock_asset_count_lower_bound(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("WAGA_SNOWFLAKE_ACCOUNT", "x")
        monkeypatch.setenv("WAGA_SNOWFLAKE_USER", "u")
        monkeypatch.setenv("WAGA_SNOWFLAKE_PRIVATE_KEY_BASE64", "a")
        monkeypatch.setenv("WAGA_SNOWFLAKE_WAREHOUSE", "w")
        monkeypatch.setenv("WAGA_SNOWFLAKE_ROLE", "r")
        monkeypatch.setenv("WAGA_MOCK_ASSET_COUNT", "0")
        with pytest.raises(ValidationError):
            _config_no_dotenv()

    def test_mock_asset_count_upper_bound(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("WAGA_SNOWFLAKE_ACCOUNT", "x")
        monkeypatch.setenv("WAGA_SNOWFLAKE_USER", "u")
        monkeypatch.setenv("WAGA_SNOWFLAKE_PRIVATE_KEY_BASE64", "a")
        monkeypatch.setenv("WAGA_SNOWFLAKE_WAREHOUSE", "w")
        monkeypatch.setenv("WAGA_SNOWFLAKE_ROLE", "r")
        monkeypatch.setenv("WAGA_MOCK_ASSET_COUNT", "101")
        with pytest.raises(ValidationError):
            _config_no_dotenv()

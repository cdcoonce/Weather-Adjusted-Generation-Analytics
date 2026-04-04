"""Unit tests for DltIngestionResource.

Validates that the resource instantiates with mock config and that
``create_pipeline()`` returns a dlt pipeline object.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from weather_analytics.resources.dlt_resource import DltIngestionResource


@pytest.mark.unit
def test_dlt_ingestion_resource_instantiates_with_config() -> None:
    """Resource should accept pipeline_name, dataset_name, and destination
    config and store them as attributes."""
    resource = DltIngestionResource(
        pipeline_name="test_pipeline",
        dataset_name="raw",
        snowflake_account="xy12345.us-east-1",
        snowflake_user="svc_waga",
        snowflake_private_key_base64="dGVzdA==",
        snowflake_warehouse="WAGA_WH",
        snowflake_database="WAGA",
        snowflake_role="WAGA_ROLE",
    )

    assert resource.pipeline_name == "test_pipeline"
    assert resource.dataset_name == "raw"
    assert resource.snowflake_account == "xy12345.us-east-1"


@pytest.mark.unit
def test_create_pipeline_returns_dlt_pipeline(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """``create_pipeline()`` should return a ``dlt.Pipeline`` object
    configured with the resource's settings."""
    resource = DltIngestionResource(
        pipeline_name="test_weather",
        dataset_name="raw",
        snowflake_account="xy12345.us-east-1",
        snowflake_user="svc_waga",
        snowflake_private_key_base64="dGVzdA==",
        snowflake_warehouse="WAGA_WH",
        snowflake_database="WAGA",
        snowflake_role="WAGA_ROLE",
    )

    # Mock both dlt.destinations.snowflake and dlt.pipeline at the module
    # level so dlt doesn't try to validate real credentials or destinations.
    fake_pipeline = MagicMock()
    fake_pipeline.pipeline_name = "test_weather"
    fake_pipeline.dataset_name = "raw"

    with (
        patch(
            "weather_analytics.resources.dlt_resource.dlt.destinations.snowflake"
        ) as mock_sf_dest,
        patch(
            "weather_analytics.resources.dlt_resource.dlt.pipeline",
            return_value=fake_pipeline,
        ) as mock_pipeline_factory,
    ):
        mock_sf_dest.return_value = "fake_destination"
        pipeline = resource.create_pipeline(
            pipelines_dir=str(tmp_path / "pipelines"),
        )

    assert pipeline.pipeline_name == "test_weather"
    assert pipeline.dataset_name == "raw"
    mock_pipeline_factory.assert_called_once()


@pytest.mark.unit
def test_create_pipeline_passes_snowflake_credentials(
    tmp_path: pytest.TempPathFactory,
) -> None:
    """``create_pipeline()`` should configure the dlt Snowflake destination
    with the correct credentials from the resource."""
    resource = DltIngestionResource(
        pipeline_name="test_creds",
        dataset_name="raw",
        snowflake_account="xy12345.us-east-1",
        snowflake_user="svc_waga",
        snowflake_private_key_base64="dGVzdA==",
        snowflake_warehouse="WAGA_WH",
        snowflake_database="WAGA",
        snowflake_role="WAGA_ROLE",
    )

    with (
        patch(
            "weather_analytics.resources.dlt_resource.dlt.destinations.snowflake"
        ) as mock_sf_dest,
        patch(
            "weather_analytics.resources.dlt_resource.dlt.pipeline",
            return_value=MagicMock(),
        ),
    ):
        mock_sf_dest.return_value = "fake_destination"
        resource.create_pipeline(
            pipelines_dir=str(tmp_path / "pipelines"),
        )

    mock_sf_dest.assert_called_once()
    call_kwargs = mock_sf_dest.call_args[1]
    creds = call_kwargs["credentials"]
    assert creds["host"] == "xy12345.us-east-1"
    assert creds["username"] == "svc_waga"
    assert creds["database"] == "WAGA"
    assert creds["warehouse"] == "WAGA_WH"
    assert creds["role"] == "WAGA_ROLE"

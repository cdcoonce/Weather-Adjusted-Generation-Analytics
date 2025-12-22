"""Phase 4 unit tests for the weather loader."""

from __future__ import annotations

from pathlib import Path

import pytest
from dlt.extract.exceptions import ResourceExtractionError

from weather_adjusted_generation_analytics.loaders import weather_loader
from tests.fixtures.parquet_builders import write_parquet


@pytest.mark.unit
@pytest.mark.io
def test_load_weather_parquet_explicit_files_yields_dict_records(
    weather_parquet_path: Path,
    weather_df,
) -> None:
    records = list(
        weather_loader.load_weather_parquet(file_paths=[weather_parquet_path])
    )

    assert len(records) == weather_df.height
    assert {"asset_id", "timestamp"}.issubset(records[0].keys())


@pytest.mark.unit
@pytest.mark.io
def test_load_weather_parquet_discovers_files_from_config_dir(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    weather_df,
) -> None:
    # Config exposes `weather_raw_path` as a derived property:
    #   weather_raw_path == data_raw / "weather"
    monkeypatch.setattr(weather_loader.config, "data_raw", tmp_path)
    weather_dir = tmp_path / "weather"

    write_parquet(weather_df, weather_dir / "weather_2023-01-01.parquet")
    write_parquet(weather_df.head(1), weather_dir / "ignore_me.parquet")

    records = list(weather_loader.load_weather_parquet(file_paths=None))

    assert len(records) == weather_df.height


@pytest.mark.unit
@pytest.mark.io
def test_load_weather_parquet_raises_on_read_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    parquet_path = tmp_path / "weather_2023-01-02.parquet"
    parquet_path.write_bytes(b"not a parquet")

    def _boom(_: Path):  # pragma: no cover
        raise ValueError("boom")

    monkeypatch.setattr(weather_loader.pl, "read_parquet", _boom)

    # dlt wraps generator exceptions as ResourceExtractionError.
    with pytest.raises(ResourceExtractionError, match="boom"):
        list(weather_loader.load_weather_parquet(file_paths=[parquet_path]))


@pytest.mark.unit
def test_run_weather_ingestion_runs_pipeline_with_jsonl_loader(
    monkeypatch: pytest.MonkeyPatch,
    mocker,
    tmp_path: Path,
) -> None:
    pipeline = mocker.Mock()
    pipeline.pipeline_name = "unit_weather_pipeline"

    load_info = mocker.Mock()
    load_info.has_failed_jobs = False
    pipeline.run = mocker.Mock(return_value=load_info)

    resource_sentinel = object()
    load_weather_parquet_mock = mocker.Mock(return_value=resource_sentinel)

    monkeypatch.setattr(weather_loader, "get_weather_pipeline", lambda: pipeline)
    monkeypatch.setattr(weather_loader, "load_weather_parquet", load_weather_parquet_mock)
    monkeypatch.setattr(weather_loader, "logger", mocker.Mock())

    file_paths = [tmp_path / "weather_2023-01-01.parquet"]
    weather_loader.run_weather_ingestion(file_paths=file_paths)

    load_weather_parquet_mock.assert_called_once_with(file_paths=file_paths)
    pipeline.run.assert_called_once_with(
        resource_sentinel,
        loader_file_format="jsonl",
    )


@pytest.mark.unit
def test_run_weather_ingestion_handles_failed_jobs_branch(
    monkeypatch: pytest.MonkeyPatch,
    mocker,
) -> None:
    pipeline = mocker.Mock()
    pipeline.pipeline_name = "unit_weather_pipeline"

    failed_job = mocker.Mock(job_file_path="job.jsonl", failed_message="nope")
    package = mocker.Mock()
    package.jobs = {"failed_jobs": [failed_job]}

    load_info = mocker.Mock()
    load_info.has_failed_jobs = True
    load_info.load_packages = [package]
    pipeline.run = mocker.Mock(return_value=load_info)

    resource_sentinel = object()
    monkeypatch.setattr(weather_loader, "get_weather_pipeline", lambda: pipeline)
    monkeypatch.setattr(
        weather_loader,
        "load_weather_parquet",
        mocker.Mock(return_value=resource_sentinel),
    )

    logger_mock = mocker.Mock()
    monkeypatch.setattr(weather_loader, "logger", logger_mock)

    weather_loader.run_weather_ingestion(file_paths=[])

    assert logger_mock.error.called

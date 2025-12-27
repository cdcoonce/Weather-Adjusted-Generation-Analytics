"""Phase 4 unit tests for the dlt pipeline orchestrator.

These tests mock dlt and ingestion functions to avoid side effects.

"""

from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import call

import pytest

from weather_adjusted_generation_analytics.loaders import dlt_pipeline


@pytest.mark.unit
def test_run_full_ingestion_calls_sub_ingestions_in_order(
    monkeypatch: pytest.MonkeyPatch,
    mocker,
    tmp_path: Path,
) -> None:
    weather_files = [tmp_path / "weather_2023-01-01.parquet"]
    generation_files = [tmp_path / "generation_2023-01-01.parquet"]

    ensure_directories_mock = mocker.Mock()
    run_weather_ingestion_mock = mocker.Mock()
    run_generation_ingestion_mock = mocker.Mock()

    # `config` is a Pydantic model instance which doesn't allow monkeypatching
    # arbitrary attributes/methods. Replace the module-level `config` with a
    # lightweight fake for this test.
    monkeypatch.setattr(
        dlt_pipeline,
        "config",
        SimpleNamespace(ensure_directories=ensure_directories_mock),
    )
    monkeypatch.setattr(
        dlt_pipeline,
        "run_weather_ingestion",
        run_weather_ingestion_mock,
    )
    monkeypatch.setattr(
        dlt_pipeline,
        "run_generation_ingestion",
        run_generation_ingestion_mock,
    )

    recorder = mocker.Mock()
    recorder.attach_mock(ensure_directories_mock, "ensure_directories")
    recorder.attach_mock(run_weather_ingestion_mock, "run_weather_ingestion")
    recorder.attach_mock(run_generation_ingestion_mock, "run_generation_ingestion")

    dlt_pipeline.run_full_ingestion(
        weather_files=weather_files,
        generation_files=generation_files,
    )

    assert recorder.mock_calls == [
        call.ensure_directories(),
        call.run_weather_ingestion(file_paths=weather_files),
        call.run_generation_ingestion(file_paths=generation_files),
    ]


@pytest.mark.unit
def test_run_combined_pipeline_builds_pipeline_and_runs_resources(
    monkeypatch: pytest.MonkeyPatch,
    mocker,
    tmp_path: Path,
) -> None:
    duckdb_path = tmp_path / "warehouse.duckdb"

    monkeypatch.setattr(dlt_pipeline.config, "dlt_pipeline_name", "test_pipeline")
    monkeypatch.setattr(dlt_pipeline.config, "dlt_schema", "test_schema")
    monkeypatch.setattr(dlt_pipeline.config, "duckdb_path", duckdb_path)

    destination_sentinel = object()
    duckdb_destination_mock = mocker.Mock(return_value=destination_sentinel)
    monkeypatch.setattr(dlt_pipeline.dlt.destinations, "duckdb", duckdb_destination_mock)

    load_info = SimpleNamespace(has_failed_jobs=False, load_packages=[])
    pipeline_instance = mocker.Mock()
    pipeline_instance.pipeline_name = "test_pipeline"
    pipeline_instance.run = mocker.Mock(return_value=load_info)

    pipeline_factory_mock = mocker.Mock(return_value=pipeline_instance)
    monkeypatch.setattr(dlt_pipeline.dlt, "pipeline", pipeline_factory_mock)

    weather_resource_sentinel = object()
    generation_resource_sentinel = object()
    load_weather_parquet_mock = mocker.Mock(return_value=weather_resource_sentinel)
    load_generation_parquet_mock = mocker.Mock(return_value=generation_resource_sentinel)
    monkeypatch.setattr(dlt_pipeline, "load_weather_parquet", load_weather_parquet_mock)
    monkeypatch.setattr(
        dlt_pipeline,
        "load_generation_parquet",
        load_generation_parquet_mock,
    )

    weather_files = [Path("weather.parquet")]
    generation_files = [Path("generation.parquet")]

    dlt_pipeline.run_combined_pipeline(
        weather_files=weather_files,
        generation_files=generation_files,
    )

    duckdb_destination_mock.assert_called_once_with(credentials=str(duckdb_path))

    pipeline_factory_mock.assert_called_once_with(
        pipeline_name="test_pipeline",
        destination=destination_sentinel,
        dataset_name="test_schema",
        progress="log",
    )

    load_weather_parquet_mock.assert_called_once_with(file_paths=weather_files)
    load_generation_parquet_mock.assert_called_once_with(file_paths=generation_files)

    pipeline_instance.run.assert_called_once_with(
        [weather_resource_sentinel, generation_resource_sentinel],
        loader_file_format="jsonl",
    )


@pytest.mark.unit
def test_run_combined_pipeline_logs_success_when_no_failed_jobs(
    monkeypatch: pytest.MonkeyPatch,
    mocker,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    duckdb_path = tmp_path / "warehouse.duckdb"

    monkeypatch.setattr(dlt_pipeline.config, "dlt_pipeline_name", "test_pipeline")
    monkeypatch.setattr(dlt_pipeline.config, "dlt_schema", "test_schema")
    monkeypatch.setattr(dlt_pipeline.config, "duckdb_path", duckdb_path)

    destination_sentinel = object()
    monkeypatch.setattr(
        dlt_pipeline.dlt.destinations,
        "duckdb",
        mocker.Mock(return_value=destination_sentinel),
    )

    load_info = SimpleNamespace(has_failed_jobs=False, load_packages=[])
    pipeline_instance = mocker.Mock()
    pipeline_instance.pipeline_name = "test_pipeline"
    pipeline_instance.run = mocker.Mock(return_value=load_info)
    monkeypatch.setattr(
        dlt_pipeline.dlt,
        "pipeline",
        mocker.Mock(return_value=pipeline_instance),
    )

    monkeypatch.setattr(dlt_pipeline, "load_weather_parquet", mocker.Mock())
    monkeypatch.setattr(dlt_pipeline, "load_generation_parquet", mocker.Mock())

    with caplog.at_level(logging.INFO):
        dlt_pipeline.run_combined_pipeline(
            weather_files=[Path("weather.parquet")],
            generation_files=[Path("generation.parquet")],
        )

    messages = [rec.getMessage() for rec in caplog.records]
    assert any("All data loaded successfully" in msg for msg in messages)


@pytest.mark.unit
def test_run_combined_pipeline_logs_failed_jobs_when_present(
    monkeypatch: pytest.MonkeyPatch,
    mocker,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    duckdb_path = tmp_path / "warehouse.duckdb"

    monkeypatch.setattr(dlt_pipeline.config, "dlt_pipeline_name", "test_pipeline")
    monkeypatch.setattr(dlt_pipeline.config, "dlt_schema", "test_schema")
    monkeypatch.setattr(dlt_pipeline.config, "duckdb_path", duckdb_path)

    destination_sentinel = object()
    monkeypatch.setattr(
        dlt_pipeline.dlt.destinations,
        "duckdb",
        mocker.Mock(return_value=destination_sentinel),
    )

    failed_job_1 = SimpleNamespace(job_file_path="a.jsonl", failed_message="bad A")
    failed_job_2 = SimpleNamespace(job_file_path="b.jsonl", failed_message="bad B")
    load_packages = [
        SimpleNamespace(jobs={"failed_jobs": [failed_job_1, failed_job_2]})
    ]
    load_info = SimpleNamespace(has_failed_jobs=True, load_packages=load_packages)

    pipeline_instance = mocker.Mock()
    pipeline_instance.pipeline_name = "test_pipeline"
    pipeline_instance.run = mocker.Mock(return_value=load_info)
    monkeypatch.setattr(
        dlt_pipeline.dlt,
        "pipeline",
        mocker.Mock(return_value=pipeline_instance),
    )

    monkeypatch.setattr(dlt_pipeline, "load_weather_parquet", mocker.Mock())
    monkeypatch.setattr(dlt_pipeline, "load_generation_parquet", mocker.Mock())

    with caplog.at_level(logging.ERROR):
        dlt_pipeline.run_combined_pipeline(
            weather_files=[Path("weather.parquet")],
            generation_files=[Path("generation.parquet")],
        )

    messages = [rec.getMessage() for rec in caplog.records]
    assert any("Combined ingestion had failures" in msg for msg in messages)
    assert any("Failed job: a.jsonl - bad A" in msg for msg in messages)
    assert any("Failed job: b.jsonl - bad B" in msg for msg in messages)


@pytest.mark.unit
def test_run_combined_pipeline_reraises_and_logs_on_exception(
    monkeypatch: pytest.MonkeyPatch,
    mocker,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    duckdb_path = tmp_path / "warehouse.duckdb"

    monkeypatch.setattr(dlt_pipeline.config, "dlt_pipeline_name", "test_pipeline")
    monkeypatch.setattr(dlt_pipeline.config, "dlt_schema", "test_schema")
    monkeypatch.setattr(dlt_pipeline.config, "duckdb_path", duckdb_path)

    destination_sentinel = object()
    monkeypatch.setattr(
        dlt_pipeline.dlt.destinations,
        "duckdb",
        mocker.Mock(return_value=destination_sentinel),
    )

    pipeline_instance = mocker.Mock()
    pipeline_instance.pipeline_name = "test_pipeline"
    pipeline_instance.run = mocker.Mock(side_effect=RuntimeError("boom"))
    monkeypatch.setattr(
        dlt_pipeline.dlt,
        "pipeline",
        mocker.Mock(return_value=pipeline_instance),
    )

    monkeypatch.setattr(dlt_pipeline, "load_weather_parquet", mocker.Mock())
    monkeypatch.setattr(dlt_pipeline, "load_generation_parquet", mocker.Mock())

    with caplog.at_level(logging.ERROR):
        with pytest.raises(RuntimeError, match="boom"):
            dlt_pipeline.run_combined_pipeline(
                weather_files=[Path("weather.parquet")],
                generation_files=[Path("generation.parquet")],
            )

    messages = [rec.getMessage() for rec in caplog.records]
    assert any("Combined ingestion failed:" in msg for msg in messages)

"""Phase 4 unit tests for the generation loader."""

from __future__ import annotations

from pathlib import Path

import pytest
from dlt.extract.exceptions import ResourceExtractionError

from src.loaders import generation_loader
from tests.fixtures.parquet_builders import write_parquet


@pytest.mark.unit
@pytest.mark.io
def test_load_generation_parquet_explicit_files_yields_dict_records(
    generation_parquet_path: Path,
    generation_df,
) -> None:
    records = list(
        generation_loader.load_generation_parquet(file_paths=[generation_parquet_path])
    )

    assert len(records) == generation_df.height
    assert {"asset_id", "timestamp"}.issubset(records[0].keys())


@pytest.mark.unit
@pytest.mark.io
def test_load_generation_parquet_discovers_files_from_config_dir(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    generation_df,
) -> None:
    # Config exposes `generation_raw_path` as a derived property:
    #   generation_raw_path == data_raw / "generation"
    monkeypatch.setattr(generation_loader.config, "data_raw", tmp_path)
    generation_dir = tmp_path / "generation"

    write_parquet(generation_df, generation_dir / "generation_2023-01-01.parquet")
    write_parquet(generation_df.head(1), generation_dir / "ignore_me.parquet")

    records = list(generation_loader.load_generation_parquet(file_paths=None))

    assert len(records) == generation_df.height


@pytest.mark.unit
@pytest.mark.io
def test_load_generation_parquet_raises_on_read_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    parquet_path = tmp_path / "generation_2023-01-02.parquet"
    parquet_path.write_bytes(b"not a parquet")

    def _boom(_: Path):  # pragma: no cover
        raise ValueError("boom")

    monkeypatch.setattr(generation_loader.pl, "read_parquet", _boom)

    # dlt wraps generator exceptions as ResourceExtractionError.
    with pytest.raises(ResourceExtractionError, match="boom"):
        list(generation_loader.load_generation_parquet(file_paths=[parquet_path]))


@pytest.mark.unit
def test_run_generation_ingestion_runs_pipeline_with_jsonl_loader(
    monkeypatch: pytest.MonkeyPatch,
    mocker,
    tmp_path: Path,
) -> None:
    pipeline = mocker.Mock()
    pipeline.pipeline_name = "unit_generation_pipeline"

    load_info = mocker.Mock()
    load_info.has_failed_jobs = False
    pipeline.run = mocker.Mock(return_value=load_info)

    resource_sentinel = object()
    load_generation_parquet_mock = mocker.Mock(return_value=resource_sentinel)

    monkeypatch.setattr(generation_loader, "get_generation_pipeline", lambda: pipeline)
    monkeypatch.setattr(
        generation_loader,
        "load_generation_parquet",
        load_generation_parquet_mock,
    )
    monkeypatch.setattr(generation_loader, "logger", mocker.Mock())

    file_paths = [tmp_path / "generation_2023-01-01.parquet"]
    generation_loader.run_generation_ingestion(file_paths=file_paths)

    load_generation_parquet_mock.assert_called_once_with(file_paths=file_paths)
    pipeline.run.assert_called_once_with(
        resource_sentinel,
        loader_file_format="jsonl",
    )


@pytest.mark.unit
def test_run_generation_ingestion_handles_failed_jobs_branch(
    monkeypatch: pytest.MonkeyPatch,
    mocker,
) -> None:
    pipeline = mocker.Mock()
    pipeline.pipeline_name = "unit_generation_pipeline"

    failed_job = mocker.Mock(job_file_path="job.jsonl", failed_message="nope")
    package = mocker.Mock()
    package.jobs = {"failed_jobs": [failed_job]}

    load_info = mocker.Mock()
    load_info.has_failed_jobs = True
    load_info.load_packages = [package]
    pipeline.run = mocker.Mock(return_value=load_info)

    resource_sentinel = object()
    monkeypatch.setattr(generation_loader, "get_generation_pipeline", lambda: pipeline)
    monkeypatch.setattr(
        generation_loader,
        "load_generation_parquet",
        mocker.Mock(return_value=resource_sentinel),
    )

    logger_mock = mocker.Mock()
    monkeypatch.setattr(generation_loader, "logger", logger_mock)

    generation_loader.run_generation_ingestion(file_paths=[])

    assert logger_mock.error.called

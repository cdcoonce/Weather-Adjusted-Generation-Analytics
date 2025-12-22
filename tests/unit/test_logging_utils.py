"""Unit tests for `src.utils.logging_utils`."""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import pytest

from src.utils.logging_utils import JSONFormatter, get_logger, log_execution_time


@pytest.mark.unit
def test_json_formatter_emits_expected_keys_and_extra_fields() -> None:
    formatter = JSONFormatter()

    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname=__file__,
        lineno=123,
        msg="hello",
        args=(),
        exc_info=None,
    )
    record.extra_fields = {"extra_fields": {"k": "v"}}

    payload = json.loads(formatter.format(record))

    assert payload["level"] == "INFO"
    assert payload["logger"] == "test_logger"
    assert payload["message"] == "hello"

    for key in ["timestamp", "module", "function", "line"]:
        assert key in payload

    # The implementation merges `extra_fields` at top level
    assert payload["extra_fields"]["k"] == "v"


@pytest.mark.unit
def test_json_formatter_includes_exception_when_present() -> None:
    formatter = JSONFormatter()

    try:
        raise ValueError("boom")
    except ValueError:
        record = logging.LogRecord(
            name="test_logger",
            level=logging.ERROR,
            pathname=__file__,
            lineno=1,
            msg="failed",
            args=(),
            exc_info=sys.exc_info(),
        )

    payload = json.loads(formatter.format(record))
    assert payload["level"] == "ERROR"
    assert "exception" in payload


@pytest.mark.unit
def test_get_logger_is_idempotent_and_uses_json_formatter() -> None:
    logger1 = get_logger("unit.test", level="INFO", log_format="json")
    logger2 = get_logger("unit.test", level="INFO", log_format="json")

    assert logger1 is logger2
    assert len(logger1.handlers) == 1
    assert isinstance(logger1.handlers[0].formatter, JSONFormatter)


@pytest.mark.unit
@pytest.mark.io
def test_get_logger_writes_to_file(tmp_path: Path) -> None:
    log_path = tmp_path / "app.log"
    logger = get_logger("unit.file", level="INFO", log_format="text", log_file=log_path)

    logger.info("hello file")

    assert log_path.exists()
    content = log_path.read_text()
    assert "hello file" in content


@pytest.mark.unit
def test_log_execution_time_logs_success(caplog: pytest.LogCaptureFixture) -> None:
    logger = get_logger("unit.exec", level="INFO", log_format="json")

    @log_execution_time(logger, "op")
    def add(a: int, b: int) -> int:
        return a + b

    with caplog.at_level(logging.INFO):
        result = add(1, 2)

    assert result == 3
    messages = [rec.getMessage() for rec in caplog.records]
    assert any("Starting op" in m for m in messages)
    assert any("Completed op" in m for m in messages)


@pytest.mark.unit
def test_log_execution_time_logs_failure(caplog: pytest.LogCaptureFixture) -> None:
    logger = get_logger("unit.exec.fail", level="INFO", log_format="json")

    @log_execution_time(logger, "op")
    def boom() -> None:
        raise RuntimeError("nope")

    with caplog.at_level(logging.ERROR):
        with pytest.raises(RuntimeError):
            boom()

    messages = [rec.getMessage() for rec in caplog.records]
    assert any("Failed op" in m for m in messages)

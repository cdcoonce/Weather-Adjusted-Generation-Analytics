"""Structured logging utilities for the WAGA pipeline."""

import json
import logging
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from functools import wraps
from pathlib import Path
from time import perf_counter
from typing import Any, Literal, ParamSpec, TypeVar

_P = ParamSpec("_P")
_R = TypeVar("_R")

_DEFAULT_LOG_LEVEL = "INFO"
_DEFAULT_LOG_FORMAT: Literal["json", "text"] = "json"


class JSONFormatter(logging.Formatter):
    """Format log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        """Serialize *record* to a JSON string.

        Parameters
        ----------
        record : logging.LogRecord
            The log record to format.

        Returns
        -------
        str
            JSON-encoded log line.
        """
        timestamp_utc = datetime.now(UTC).isoformat().replace("+00:00", "Z")

        log_data: dict[str, Any] = {
            "timestamp": timestamp_utc,
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        if hasattr(record, "extra_fields"):
            log_data.update(record.extra_fields)  # type: ignore[arg-type]

        return json.dumps(log_data)


def get_logger(
    name: str,
    level: str = _DEFAULT_LOG_LEVEL,
    log_format: Literal["json", "text"] = _DEFAULT_LOG_FORMAT,
    log_file: Path | None = None,
) -> logging.Logger:
    """Create or retrieve a logger with standardised configuration.

    Parameters
    ----------
    name : str
        Logger name (typically ``__name__``).
    level : str
        Logging level (e.g. ``"DEBUG"``, ``"INFO"``).
    log_format : Literal["json", "text"]
        Output format.
    log_file : Path | None
        Optional file path for an additional file handler.

    Returns
    -------
    logging.Logger
        Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    logger.handlers.clear()

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, level.upper()))

    if log_format == "json":
        formatter: logging.Formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(getattr(logging, level.upper()))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def log_execution_time(
    logger: logging.Logger,
    operation: str,
) -> Callable[[Callable[_P, _R]], Callable[_P, _R]]:
    """Return a decorator that logs wall-clock execution time.

    Parameters
    ----------
    logger : logging.Logger
        Logger to write to.
    operation : str
        Human-readable label for the operation being timed.

    Returns
    -------
    Callable[[Callable[_P, _R]], Callable[_P, _R]]
        Decorator function.
    """

    def decorator(func: Callable[_P, _R]) -> Callable[_P, _R]:
        @wraps(func)
        def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
            start_time = perf_counter()
            logger.info("Starting %s", operation)

            try:
                result = func(*args, **kwargs)
                elapsed = perf_counter() - start_time
                logger.info(
                    "Completed %s",
                    operation,
                    extra={
                        "extra_fields": {
                            "operation": operation,
                            "duration_seconds": round(elapsed, 3),
                        }
                    },
                )
                return result  # noqa: TRY300
            except Exception as exc:
                elapsed = perf_counter() - start_time
                logger.exception(
                    "Failed %s",
                    operation,
                    extra={
                        "extra_fields": {
                            "operation": operation,
                            "duration_seconds": round(elapsed, 3),
                            "error": str(exc),
                        }
                    },
                )
                raise

        return wrapper

    return decorator


__all__ = ["JSONFormatter", "get_logger", "log_execution_time"]

"""Logging utilities for the renewable performance pipeline."""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from src.config import config


class JSONFormatter(logging.Formatter):
    """
    Custom JSON formatter for structured logging.

    Outputs log records as JSON objects with timestamp, level,
    logger name, message, and any additional context fields.

    """

    def format(self, record: logging.LogRecord) -> str:
        """
        Format a log record as a JSON string.

        Parameters
        ----------
        record : logging.LogRecord
            The log record to format

        Returns
        -------
        str
            JSON-formatted log string

        """
        # Use a timezone-aware UTC timestamp.
        # `datetime.utcnow()` is deprecated in Python 3.12.
        timestamp_utc = datetime.now(timezone.utc).isoformat().replace(
            "+00:00", "Z"
        )

        log_data: dict[str, Any] = {
            "timestamp": timestamp_utc,
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Add any extra fields
        if hasattr(record, "extra_fields"):
            log_data.update(record.extra_fields)

        return json.dumps(log_data)


def get_logger(
    name: str,
    level: str | None = None,
    log_format: Literal["json", "text"] | None = None,
    log_file: Path | None = None,
) -> logging.Logger:
    """
    Get or create a logger with standardized configuration.

    Creates a logger with appropriate handlers and formatters based on
    the application configuration. Supports both JSON and text output
    formats, with optional file logging.

    Parameters
    ----------
    name : str
        Name of the logger (typically __name__ of the calling module)
    level : str, optional
        Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        Defaults to config.log_level
    log_format : Literal["json", "text"], optional
        Output format for log messages. Defaults to config.log_format
    log_file : Path, optional
        Path to log file. If None, only logs to stdout

    Returns
    -------
    logging.Logger
        Configured logger instance

    Examples
    --------
    >>> logger = get_logger(__name__)
    >>> logger.info("Processing started", extra={"extra_fields": {"asset_count": 10}})

    """
    # Use config defaults if not specified
    level = level or config.log_level
    log_format = log_format or config.log_format

    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))

    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(getattr(logging, level.upper()))

    # Set formatter based on format type
    if log_format == "json":
        formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Add file handler if specified
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(getattr(logging, level.upper()))
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def log_execution_time(logger: logging.Logger, operation: str) -> Any:
    """
    Decorator to log execution time of a function.

    Parameters
    ----------
    logger : logging.Logger
        Logger instance to use for logging
    operation : str
        Description of the operation being timed

    Returns
    -------
    Callable
        Decorated function

    Examples
    --------
    >>> logger = get_logger(__name__)
    >>> @log_execution_time(logger, "data loading")
    ... def load_data():
    ...     # Load data logic
    ...     pass

    """
    from functools import wraps
    from time import perf_counter

    def decorator(func: Any) -> Any:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            start_time = perf_counter()
            logger.info(f"Starting {operation}")

            try:
                result = func(*args, **kwargs)
                elapsed = perf_counter() - start_time
                logger.info(
                    f"Completed {operation}",
                    extra={
                        "extra_fields": {
                            "operation": operation,
                            "duration_seconds": round(elapsed, 3),
                        }
                    },
                )
                return result
            except Exception as e:
                elapsed = perf_counter() - start_time
                logger.error(
                    f"Failed {operation}",
                    extra={
                        "extra_fields": {
                            "operation": operation,
                            "duration_seconds": round(elapsed, 3),
                            "error": str(e),
                        }
                    },
                    exc_info=True,
                )
                raise

        return wrapper

    return decorator

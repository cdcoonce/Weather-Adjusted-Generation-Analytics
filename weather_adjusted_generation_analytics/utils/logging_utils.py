"""Logging utilities for the renewable performance pipeline."""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from weather_adjusted_generation_analytics.config import config


class JSONFormatter(logging.Formatter):
	"""Custom JSON formatter for structured logging."""

	def format(self, record: logging.LogRecord) -> str:
		"""Format a log record as a JSON string."""
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

		if record.exc_info:
			log_data["exception"] = self.formatException(record.exc_info)

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

	Parameters
	----------
	name : str
		Logger name (typically __name__)
	level : str, optional
		Logging level. Defaults to config.log_level
	log_format : Literal["json", "text"], optional
		Output format. Defaults to config.log_format
	log_file : Path, optional
		Optional file to log to

	Returns
	-------
	logging.Logger
		Configured logger

	"""
	level = level or config.log_level
	log_format = log_format or config.log_format

	logger = logging.getLogger(name)
	logger.setLevel(getattr(logging, level.upper()))
	logger.handlers.clear()

	console_handler = logging.StreamHandler(sys.stdout)
	console_handler.setLevel(getattr(logging, level.upper()))

	if log_format == "json":
		formatter = JSONFormatter()
	else:
		formatter = logging.Formatter(
			fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
			datefmt="%Y-%m-%d %H:%M:%S",
		)

	console_handler.setFormatter(formatter)
	logger.addHandler(console_handler)

	if log_file:
		log_file.parent.mkdir(parents=True, exist_ok=True)
		file_handler = logging.FileHandler(log_file)
		file_handler.setLevel(getattr(logging, level.upper()))
		file_handler.setFormatter(formatter)
		logger.addHandler(file_handler)

	return logger


def log_execution_time(logger: logging.Logger, operation: str) -> Any:
	"""Decorator factory that logs execution timing for a function."""
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
			except Exception as exc:
				elapsed = perf_counter() - start_time
				logger.error(
					f"Failed {operation}",
					extra={
						"extra_fields": {
							"operation": operation,
							"duration_seconds": round(elapsed, 3),
							"error": str(exc),
						}
					},
					exc_info=True,
				)
				raise

		return wrapper

	return decorator


__all__ = ["JSONFormatter", "get_logger", "log_execution_time"]


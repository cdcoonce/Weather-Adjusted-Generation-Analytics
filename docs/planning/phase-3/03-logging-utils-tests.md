# Phase 3.3 — `logging_utils` Unit Tests

## Objective
Validate structured logging behavior in `src/utils/logging_utils.py`.

## Where tests should live
- `tests/unit/test_logging_utils.py`

## Tools/fixtures to use
- `caplog` (pytest built-in) to capture log records.
- `monkeypatch` to isolate environment/config impacts.

## Test plan

### `JSONFormatter.format(record)`
- Basic structure:
  - Output should be valid JSON.
  - Keys should exist: `timestamp`, `level`, `logger`, `message`, `module`, `function`, `line`.
- `extra_fields` merge:
  - Create a `LogRecord` with `record.extra_fields = {"k": "v"}` and assert it shows up in JSON.
- Exception formatting:
  - Provide `exc_info` on the record and assert `exception` is present.

### `get_logger(name, level=None, log_format=None, log_file=None)`
- Handler configuration:
  - Calling twice should not create duplicate handlers (the function clears handlers).
  - With `log_format="json"`, the handler formatter should be `JSONFormatter`.
  - With `log_format="text"`, formatter should be `logging.Formatter`.
- Level:
  - When passing `level="DEBUG"`, logger and handler levels should be DEBUG.
- Optional file logging:
  - Provide a `tmp_path` file path and assert the file is created and receives log output.
  - Mark this test `@pytest.mark.io`.

### `log_execution_time(logger, operation)`
- Success path:
  - Wrap a small function that returns a value.
  - Assert logs contain “Starting …” and “Completed …” and include duration fields.
- Failure path:
  - Wrap a function that raises.
  - Assert an ERROR log exists with `error` field.

## Notes on determinism
- Do not assert exact timestamps.
- If asserting duration, assert presence/type rather than exact value.

## Acceptance criteria
- Tests confirm structure and configuration, not styling.
- Tests are robust across Python versions and platforms.

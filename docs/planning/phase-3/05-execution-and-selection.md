# Phase 3.5 — Execution & Selection

## Objective
Define the commands and markers used to run Phase 3 tests.

## Commands

### Run all unit tests

```bash
uv run pytest -m unit
```

### Run only the new module tests

```bash
uv run pytest -m unit tests/unit/test_polars_utils.py
uv run pytest -m unit tests/unit/test_logging_utils.py
uv run pytest -m unit tests/unit/test_settings.py
```

### Exclude IO-heavy unit tests
(Usually not necessary, but useful for very fast iterations once IO tests grow.)

```bash
uv run pytest -m "unit and not io"
```

## Marker usage rules
- Every Phase 3 test: `@pytest.mark.unit`
- Any test that touches filesystem: also `@pytest.mark.io`

## Coverage expectations
- Coverage should increase meaningfully for:
  - `weather_adjusted_generation_analytics/config/settings.py`
  - `weather_adjusted_generation_analytics/utils/polars_utils.py`
  - `weather_adjusted_generation_analytics/utils/logging_utils.py`

We do not set a minimum threshold yet; that comes when CI is introduced.

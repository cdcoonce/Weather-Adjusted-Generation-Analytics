from pathlib import Path

import pytest

from weather_analytics.cockpit.data import Dataset, load_dataset

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def dataset() -> Dataset:
    """The trimmed 2-asset x 2-day dataset from tests/cockpit/fixtures/."""
    return load_dataset(FIXTURES)

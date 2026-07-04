from weather_analytics.cockpit.data import Dataset


def test_load_dataset_parses_all_four_files(dataset: Dataset) -> None:
    assert dataset.manifest.asset_count == 2
    assert dataset.manifest.date_range_start == "2026-07-01"
    assert dataset.manifest.date_range_end == "2026-07-02"
    assert {a.asset_id for a in dataset.assets} == {"W1", "S1"}
    assert len(dataset.daily) == 4
    assert len(dataset.weather) == 4


def test_asset_types_normalized(dataset: Dataset) -> None:
    by_id = {a.asset_id: a for a in dataset.assets}
    assert by_id["W1"].asset_type == "wind"
    assert by_id["S1"].asset_type == "solar"


def test_raw_holds_all_four_payloads(dataset: Dataset) -> None:
    assert set(dataset.raw) == {"manifest", "assets", "daily", "weather"}
    assert isinstance(dataset.raw["daily"], list)
    assert dataset.raw["manifest"]["schema_version"] == "1.0"

from weather_analytics.cockpit import charts


def test_fleet_kpis_all(dataset):
    kpis = {k["key"]: k for k in charts.fleet_kpis(dataset)}
    # net generation = 800+900+300+320 = 2320 MWh
    assert "2,320" in kpis["net_generation"]["value"]
    # curtailment = 10+5+0+0 = 15 MWh
    assert "15" in kpis["curtailment"]["value"]
    # capacity factor = mean(0.33,0.38,0.25,0.27) = 0.3075 -> "30.8%" (pct, 1 dp)
    assert kpis["capacity_factor"]["value"].endswith("%")


def test_fleet_kpis_filtered_by_asset(dataset):
    kpis = {k["key"]: k for k in charts.fleet_kpis(dataset, asset_ids={"S1"})}
    # S1 only: net gen = 300+320 = 620
    assert "620" in kpis["net_generation"]["value"]


def test_generation_series_shape(dataset):
    s = charts.generation_series(dataset)
    assert s is not None
    assert s["polyline"]  # non-empty points string
    assert s["y_max"] > 0
    assert s["x0_label"] == "2026-07-01"
    assert s["x1_label"] == "2026-07-02"


def test_asset_bars_pct_of_max(dataset):
    bars = {b["label"]: b for b in charts.asset_bars(dataset)}
    # W1 mean CF = 0.355 (max), S1 mean CF = 0.26 -> W1 pct == 100
    assert bars["W1"]["pct"] == 100
    assert bars["W1"]["asset_type"] == "wind"
    assert bars["S1"]["pct"] < 100


def test_type_split_wind_vs_solar(dataset):
    split = {s["label"]: s for s in charts.type_split(dataset)}
    assert set(split) == {"wind", "solar"}
    # wind = 1700, solar = 620 -> wind is max
    assert split["wind"]["pct"] == 100


def test_empty_series_returns_none(dataset):
    assert charts.generation_series(dataset, start="2030-01-01") is None

from weather_analytics.cockpit import config


def test_config_constants():
    assert config.CF_PROJECT_NAME == "waga-dashboard"
    assert config.SITE_URL == "https://waga-dashboard.pages.dev"
    assert config.DEFAULT_EXPORT_DIR == "dashboard_exports"
    assert config.DEFAULT_OUT == "dist/index.html"

"""Unit tests for ``weather_analytics.checks.dashboard``."""


from unittest.mock import MagicMock, patch

import pytest
from dagster import AssetCheckResult
from github.GithubException import GithubException

from weather_analytics.checks.dashboard import waga_dashboard_export_commit_landed


def _make_mock_portfolio_repo() -> MagicMock:
    mock_resource = MagicMock()
    mock_resource.full_name = "cdcoonce/charleslikesdata"
    mock_resource.branch = "main"
    mock_resource.token = "fake-pat"
    return mock_resource


@pytest.mark.unit
def test_commit_landed_passes_when_ref_resolves() -> None:
    """Check passes when the GitHub ref resolves successfully."""
    mock_resource = _make_mock_portfolio_repo()

    mock_ref = MagicMock()
    mock_ref.object.sha = "abc123"
    mock_repo = MagicMock()
    mock_repo.get_git_ref.return_value = mock_ref
    mock_gh = MagicMock()
    mock_gh.get_repo.return_value = mock_repo

    with patch(
        "weather_analytics.checks.dashboard.Github",
        return_value=mock_gh,
    ):
        result = waga_dashboard_export_commit_landed(portfolio_repo=mock_resource)

    assert isinstance(result, AssetCheckResult)
    assert result.passed is True
    assert result.metadata["commit_sha"].value == "abc123"
    assert result.metadata["branch"].value == "main"


@pytest.mark.unit
def test_commit_landed_fails_on_api_error() -> None:
    """Check fails when the GitHub API raises GithubException."""

    mock_resource = _make_mock_portfolio_repo()

    mock_repo = MagicMock()
    mock_repo.get_git_ref.side_effect = GithubException(status=404, data={})
    mock_gh = MagicMock()
    mock_gh.get_repo.return_value = mock_repo

    with patch(
        "weather_analytics.checks.dashboard.Github",
        return_value=mock_gh,
    ):
        result = waga_dashboard_export_commit_landed(portfolio_repo=mock_resource)

    assert isinstance(result, AssetCheckResult)
    assert result.passed is False
    assert "error" in result.metadata

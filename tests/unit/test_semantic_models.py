"""Unit tests for dbt semantic models YAML definition."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

_SEMANTIC_MODELS_PATH = (
    Path(__file__).resolve().parents[2]
    / "dbt"
    / "renewable_dbt"
    / "models"
    / "semantic_models"
    / "_semantic_models.yml"
)


@pytest.mark.unit
@pytest.mark.dbt
class TestSemanticModelsYAML:
    """Validate the semantic models definition file."""

    @pytest.fixture
    def parsed(self) -> dict:
        assert _SEMANTIC_MODELS_PATH.exists(), (
            f"Semantic models file not found: {_SEMANTIC_MODELS_PATH}"
        )
        with _SEMANTIC_MODELS_PATH.open() as fh:
            return yaml.safe_load(fh)

    def test_file_is_valid_yaml(self, parsed: dict) -> None:
        assert isinstance(parsed, dict)

    def test_has_semantic_models_key(self, parsed: dict) -> None:
        assert "semantic_models" in parsed
        assert len(parsed["semantic_models"]) > 0

    def test_has_metrics_key(self, parsed: dict) -> None:
        assert "metrics" in parsed
        assert len(parsed["metrics"]) > 0

    def test_references_correct_model(self, parsed: dict) -> None:
        model_refs = {sm["model"] for sm in parsed["semantic_models"]}
        assert "ref('mart_asset_performance_daily')" in model_refs

    def test_semantic_models_have_required_sections(self, parsed: dict) -> None:
        for sm in parsed["semantic_models"]:
            assert "name" in sm
            assert "entities" in sm
            assert "dimensions" in sm
            assert "measures" in sm

    def test_metrics_have_required_sections(self, parsed: dict) -> None:
        for metric in parsed["metrics"]:
            assert "name" in metric
            assert "type" in metric
            assert "type_params" in metric

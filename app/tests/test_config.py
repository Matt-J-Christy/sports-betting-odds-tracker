"""Tests for YAML configuration loader and environment variable overrides."""

import os
from pathlib import Path
from unittest.mock import patch

from app.config.config import AppConfig, _interpolate_env, load_config


def test_load_config_standard():
    config = load_config()
    assert isinstance(config, AppConfig)
    assert config.the_odds_api.base_url == "https://api.the-odds-api.com"
    assert "americanfootball_nfl" in config.the_odds_api.sports
    assert "fanduel" in [b.lower() for b in config.the_odds_api.bookmakers]
    assert config.gcp.tables.odds_table == "daily_odds_data"
    assert config.gcp.full_odds_table == f"{config.gcp.project_id}.{config.gcp.dataset_id}.daily_odds_data"


def test_load_config_from_temp_yaml(tmp_path: Path):
    yaml_content = """
app:
  name: "custom-tracker"
  environment: "staging"

the_odds_api:
  sports:
    - "basketball_nba"
  bookmakers:
    - "draftkings"
  markets: "h2h,spreads"

gcp:
  project_id: "custom-project"
  dataset_id: "custom_dataset"
  tables:
    odds_table: "custom_odds"
"""
    yaml_file = tmp_path / "custom_config.yaml"
    yaml_file.write_text(yaml_content, encoding="utf-8")

    config = load_config(config_path=yaml_file)
    assert config.name == "custom-tracker"
    assert config.environment == "staging"
    assert config.the_odds_api.sports == ["basketball_nba"]
    assert config.the_odds_api.bookmakers == ["draftkings"]
    assert config.the_odds_api.markets == "h2h,spreads"
    assert config.gcp.project_id == "custom-project"
    assert config.gcp.dataset_id == "custom_dataset"
    assert config.gcp.tables.odds_table == "custom_odds"


def test_load_config_env_overrides(tmp_path: Path):
    yaml_content = """
gcp:
  project_id: "yaml-project"
  dataset_id: "yaml-dataset"
"""
    yaml_file = tmp_path / "env_override_config.yaml"
    yaml_file.write_text(yaml_content, encoding="utf-8")

    with patch.dict(os.environ, {"GCP_PROJECT_ID": "env-override-project"}):
        config = load_config(config_path=yaml_file)
        assert config.gcp.project_id == "env-override-project"
        assert config.gcp.dataset_id == "yaml-dataset"


def test_env_interpolation():
    with patch.dict(os.environ, {"MY_TEST_VAR": "interpolated_value"}):
        raw = "project: ${MY_TEST_VAR}\nfallback: ${NON_EXISTENT_VAR:-default_val}"
        res = _interpolate_env(raw)
        assert "project: interpolated_value" in res
        assert "fallback: default_val" in res


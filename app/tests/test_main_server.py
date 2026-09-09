"""Tests for the serverless main.py ingestion entrypoint."""

from unittest.mock import patch

import pandas as pd

from main import run_ingestion_pipeline


def test_run_ingestion_pipeline_success():
    mock_df = pd.DataFrame(
        [
            {
                "snapshot_time": "2026-09-07T12:00:00Z",
                "game_id": "game_1",
                "sport": "americanfootball_nfl",
                "away_team": "Bills",
                "home_team": "Chiefs",
                "commence_time": "2026-09-10T20:00:00Z",
                "market_type": "spreads",
                "team_or_outcome": "Bills",
                "price": -110,
                "market_point": -2.5,
            }
        ]
    )

    with patch("main.my_api_key", "test_key"), \
         patch("app.query_betting_api.OddsQuery.get_new_odds", return_value=mock_df), \
         patch("app.query_betting_api.OddsQuery.write_to_bq") as mock_write_bq:

        result, status_code = run_ingestion_pipeline(sports=["americanfootball_nfl"])

        assert status_code == 200
        assert result["status"] == "success"
        assert result["rows_ingested"] == 1
        assert mock_write_bq.called


def test_run_ingestion_pipeline_missing_api_key():
    with patch("main.my_api_key", None):
        result, status_code = run_ingestion_pipeline()
        assert status_code == 500
        assert result["status"] == "error"
        assert "THE_ODDS_API_KEY" in result["message"]


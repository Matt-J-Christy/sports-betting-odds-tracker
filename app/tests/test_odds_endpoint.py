"""Tests for The Odds API client and ingestion pipeline."""

from unittest.mock import MagicMock, patch

import pandas as pd

from app.query_betting_api import OddsQuery
from app.tests.test_the_odds_api_parsing import SAMPLE_EVENT


def test_odds_query_initialization():
    query = OddsQuery(api_key="test_key")
    assert query.api_key == "test_key"
    assert query.base_url == "https://api.the-odds-api.com"
    assert query.last_quota_info == {}


def test_odds_query_get_new_odds_mocked():
    query = OddsQuery(api_key="test_key")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [SAMPLE_EVENT]
    mock_response.headers = {
        "x-requests-remaining": "480",
        "x-requests-used": "20",
    }

    with patch("requests.get", return_value=mock_response) as mock_get:
        df = query.get_new_odds(sports=["americanfootball_nfl"], test_mode=False)

        assert mock_get.called
        call_args, call_kwargs = mock_get.call_args
        assert "americanfootball_nfl" in call_args[0]
        assert call_kwargs["params"]["apiKey"] == "test_key"
        assert call_kwargs["params"]["markets"] == "h2h,spreads,totals"
        assert call_kwargs["params"]["oddsFormat"] == "american"

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 4
        assert query.last_quota_info["americanfootball_nfl"]["requests_remaining"] == "480"


def test_odds_query_multi_sport_mocked():
    query = OddsQuery(api_key="test_key")

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [SAMPLE_EVENT]
    mock_response.headers = {"x-requests-remaining": "478", "x-requests-used": "22"}

    with patch("requests.get", return_value=mock_response) as mock_get:
        df = query.get_new_odds(
            sports=["americanfootball_nfl", "americanfootball_ncaaf"],
            test_mode=False,
        )
        assert mock_get.call_count == 2
        assert len(df) == 8  # 4 rows * 2 sports

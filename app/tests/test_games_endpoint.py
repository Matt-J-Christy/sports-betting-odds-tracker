"""Unit tests for the ESPN scoreboard metadata and score client."""

import datetime
from unittest.mock import MagicMock, patch

from app.query_games_api import GamesQuery

ESPN_EVENT = {
    "id": "401671234",
    "date": "2026-09-10T00:20:00Z",
    "status": {"type": {"completed": True}},
    "competitions": [
        {
            "competitors": [
                {"homeAway": "home", "score": "24", "team": {"displayName": "Chiefs"}},
                {"homeAway": "away", "score": "20", "team": {"displayName": "Bills"}},
            ]
        }
    ],
}


def test_espn_metadata_request_and_normalization():
    response = MagicMock()
    response.json.return_value = {"events": [ESPN_EVENT]}
    response.raise_for_status.return_value = None

    with patch("app.query_games_api.requests.get", return_value=response) as mock_get:
        frame = GamesQuery().query_game_metadata(datetime.date(2026, 9, 10))

    assert len(frame) == 2
    assert list(frame.columns) == [
        "event_id",
        "sport_key",
        "sport_title",
        "commence_time",
        "home_team",
        "away_team",
    ]
    assert mock_get.call_args_list[0].args[0].startswith("https://site.api.espn.com/")
    assert mock_get.call_args_list[0].kwargs["params"]["dates"] == "20260910"


def test_espn_scores_normalization():
    response = MagicMock()
    response.json.return_value = {"events": [ESPN_EVENT]}
    response.raise_for_status.return_value = None

    with patch("app.query_games_api.requests.get", return_value=response):
        frame = GamesQuery().query_game_scores()

    assert bool(frame.loc[0, "completed"]) is True
    assert frame.loc[0, "home_score"] == 24
    assert frame.loc[0, "away_score"] == 20

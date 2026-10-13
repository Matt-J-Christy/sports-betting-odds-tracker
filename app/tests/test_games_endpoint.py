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


def test_upsert_to_bq_metadata_success():
    import pandas as pd

    df = pd.DataFrame(
        [
            {
                "event_id": "401671234",
                "sport_key": "espn_nfl",
                "sport_title": "NFL",
                "commence_time": "2026-09-10T00:20:00Z",
                "home_team": "Chiefs",
                "away_team": "Bills",
            }
        ]
    )

    mock_client = MagicMock()
    mock_load_job = MagicMock()
    mock_query_job = MagicMock()
    mock_query_job.num_dml_affected_rows = 1

    mock_client.load_table_from_dataframe.return_value = mock_load_job
    mock_client.query.return_value = mock_query_job

    with patch("app.query_games_api.bigquery.Client", return_value=mock_client):
        affected = GamesQuery().upsert_to_bq(df, table_type="metadata")

    assert affected == 1
    assert mock_client.create_table.called
    assert mock_client.load_table_from_dataframe.called
    assert mock_client.query.called
    assert mock_client.delete_table.called


def test_upsert_to_bq_empty_dataframe():
    import pandas as pd

    df = pd.DataFrame()
    with patch("app.query_games_api.bigquery.Client") as mock_client:
        affected = GamesQuery().upsert_to_bq(df, table_type="metadata")

    assert affected == 0
    assert not mock_client.called


def test_upsert_to_bq_invalid_table_type():
    import pandas as pd
    import pytest

    df = pd.DataFrame([{"event_id": "1"}])
    with pytest.raises(ValueError, match="Unsupported table_type"):
        GamesQuery().upsert_to_bq(df, table_type="invalid_type")


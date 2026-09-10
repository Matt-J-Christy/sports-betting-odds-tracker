import pandas as pd

from app.query_betting_api import OddsQuery

SAMPLE_EVENT = {
    "id": "evt_123",
    "sport_key": "americanfootball_nfl",
    "home_team": "Kansas City Chiefs",
    "away_team": "Buffalo Bills",
    "commence_time": "2026-09-01T20:00:00Z",
    "bookmakers": [
        {
            "key": "fanduel",
            "markets": [
                {
                    "key": "spreads",
                    "outcomes": [
                        {"name": "Buffalo Bills", "price": -110, "point": -2.5},
                        {"name": "Kansas City Chiefs", "price": 100, "point": 2.5},
                    ],
                },
                {
                    "key": "h2h",
                    "outcomes": [
                        {"name": "Buffalo Bills", "price": 140},
                        {"name": "Kansas City Chiefs", "price": -170},
                    ],
                },
            ],
        }
    ],
}


def test_odds_query_extracts_fanduel_rows():
    query = OddsQuery(api_key="test-key", gcp_service_accnt=None)

    df = query._normalize_event_rows([SAMPLE_EVENT])

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == [
        "snapshot_time",
        "game_id",
        "sport",
        "away_team",
        "home_team",
        "commence_time",
        "market_type",
        "team_or_outcome",
        "price",
        "market_point",
    ]
    assert df["market_type"].tolist() == ["spreads", "spreads", "h2h", "h2h"]
    assert df["team_or_outcome"].tolist() == [
        "Buffalo Bills",
        "Kansas City Chiefs",
        "Buffalo Bills",
        "Kansas City Chiefs",
    ]


def test_event_metadata_is_provider_native_and_deduplicated():
    query = OddsQuery(api_key="test-key", gcp_service_accnt=None)

    df = query._normalize_event_metadata(
        [
            {
                "id": "evt_123",
                "sport_key": "americanfootball_nfl",
                "sport_title": "NFL",
                "home_team": "Kansas City Chiefs",
                "away_team": "Buffalo Bills",
                "commence_time": "2026-09-01T20:00:00Z",
            },
            {"id": "evt_123"},
        ]
    )

    assert list(df.columns) == [
        "event_id",
        "sport_key",
        "sport_title",
        "commence_time",
        "home_team",
        "away_team",
    ]
    assert len(df) == 1
    assert "game_stage" not in df.columns


def test_score_normalizer_handles_missing_scores():
    query = OddsQuery(api_key="test-key", gcp_service_accnt=None)

    df = query._normalize_score_rows(
        [
            {
                "id": "evt_123",
                "sport_key": "americanfootball_nfl",
                "sport_title": "NFL",
                "home_team": "Kansas City Chiefs",
                "away_team": "Buffalo Bills",
                "completed": False,
                "scores": [],
            }
        ]
    )

    assert list(df.columns) == [
        "event_id",
        "sport_key",
        "sport_title",
        "commence_time",
        "completed",
        "last_update",
        "home_team",
        "away_team",
        "home_score",
        "away_score",
    ]
    assert pd.isna(df.loc[0, "home_score"])
    assert pd.isna(df.loc[0, "away_score"])

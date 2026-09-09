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

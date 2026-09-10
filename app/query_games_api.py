"""Query ESPN scoreboard endpoints for game metadata and final scores."""

from __future__ import annotations

import datetime
import os
import sys
from typing import Any

import pandas as pd
import requests
from google.cloud import storage
from google.oauth2 import service_account

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))

from app.config.config import gcp_service_accnt, my_api_key  # noqa: E402

ESPN_SCOREBOARD_URLS = {
    "nfl": "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard",
    "ncaaf": (
        "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard"
    ),
}


class GamesQuery:
    """Fetch provider-native event metadata and scores from ESPN."""

    def __init__(
        self,
        api_key: str | None = None,
        gcp_service_accnt: service_account.Credentials | None = None,
    ) -> None:
        self.gcp_service_accnt = gcp_service_accnt

    @staticmethod
    def _event_columns() -> list[str]:
        return [
            "event_id",
            "sport_key",
            "sport_title",
            "commence_time",
            "home_team",
            "away_team",
        ]

    @staticmethod
    def _score_columns() -> list[str]:
        return [
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

    @staticmethod
    def _competitors(event: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        competitions = event.get("competitions", [])
        competitors = competitions[0].get("competitors", []) if competitions else []
        home = next((item for item in competitors if item.get("homeAway") == "home"), None)
        away = next((item for item in competitors if item.get("homeAway") == "away"), None)
        return home, away

    @staticmethod
    def _team_name(competitor: dict[str, Any] | None) -> str | None:
        if not competitor:
            return None
        team = competitor.get("team", {})
        return team.get("displayName") or competitor.get("displayName")

    @staticmethod
    def _score(competitor: dict[str, Any] | None) -> int | None:
        try:
            return int(competitor["score"]) if competitor and competitor.get("score") is not None else None
        except (TypeError, ValueError):
            return None

    def _fetch_events(self, sport: str, date: datetime.date | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": 100}
        if date is not None:
            params["dates"] = date.strftime("%Y%m%d")
        response = requests.get(ESPN_SCOREBOARD_URLS[sport], params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        events = payload.get("events", []) if isinstance(payload, dict) else []
        return [event for event in events if isinstance(event, dict)]

    def _normalize_metadata(self, events: list[dict[str, Any]], sport: str) -> pd.DataFrame:
        rows = []
        for event in events:
            home, away = self._competitors(event)
            rows.append(
                {
                    "event_id": str(event["id"]),
                    "sport_key": f"espn_{sport}",
                    "sport_title": "NCAA Football" if sport == "ncaaf" else "NFL",
                    "commence_time": event.get("date"),
                    "home_team": self._team_name(home),
                    "away_team": self._team_name(away),
                }
            )
        return pd.DataFrame(rows, columns=self._event_columns())

    def _normalize_scores(self, events: list[dict[str, Any]], sport: str) -> pd.DataFrame:
        rows = []
        for event in events:
            home, away = self._competitors(event)
            status = event.get("status", {}).get("type", {})
            rows.append(
                {
                    "event_id": str(event["id"]),
                    "sport_key": f"espn_{sport}",
                    "sport_title": "NCAA Football" if sport == "ncaaf" else "NFL",
                    "commence_time": event.get("date"),
                    "completed": bool(status.get("completed", False)),
                    "last_update": None,
                    "home_team": self._team_name(home),
                    "away_team": self._team_name(away),
                    "home_score": self._score(home),
                    "away_score": self._score(away),
                }
            )
        return pd.DataFrame(rows, columns=self._score_columns())

    def query_game_metadata(self, date: datetime.date | None = None) -> pd.DataFrame:
        frames = [
            self._normalize_metadata(self._fetch_events(sport, date), sport)
            for sport in ESPN_SCOREBOARD_URLS
        ]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=self._event_columns())

    def query_game_scores(self, date: datetime.date | None = None) -> pd.DataFrame:
        frames = [
            self._normalize_scores(self._fetch_events(sport, date), sport)
            for sport in ESPN_SCOREBOARD_URLS
        ]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=self._score_columns())

    def write_to_gcs(
        self,
        data: pd.DataFrame,
        data_topic: str,
        date: datetime.date | None = None,
    ) -> None:
        write_date = str(date or datetime.date.today())
        filename = {"metadata": "game_metadata.csv", "scores": "score_data.csv"}.get(data_topic)
        if filename is None:
            raise ValueError(f"Unsupported ESPN data topic: {data_topic}")
        storage_client = storage.Client(credentials=self.gcp_service_accnt)
        bucket = storage_client.bucket("odds-data")
        blob = bucket.blob(f"game_data_{write_date}/{filename}")
        blob.upload_from_string(data.to_csv(index=False), content_type="text/csv")
        print(f"Wrote {len(data)} rows to odds-data/game_data_{write_date}/{filename}")


if __name__ == "__main__":
    query = GamesQuery(api_key=my_api_key, gcp_service_accnt=gcp_service_accnt)
    query.write_to_gcs(query.query_game_scores(), "scores")
    query.write_to_gcs(query.query_game_metadata(), "metadata")

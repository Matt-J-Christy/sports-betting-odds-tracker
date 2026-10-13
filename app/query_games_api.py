"""Query ESPN scoreboard endpoints for game metadata and final scores."""

from __future__ import annotations

import datetime
import os
import sys
from typing import Any

import pandas as pd
import requests
from google.cloud import bigquery, storage
from google.oauth2 import service_account

sys.path.append(os.path.join(os.path.dirname(__file__), "../"))

from app.config.config import (  # noqa: E402
    DATASET_ID,
    GCP_PROJECT_ID,
    gcp_service_accnt,
    my_api_key,
)
from app.ddl.table_schemas import (  # noqa: E402
    game_metadata_schema,
    game_score_schema,
    ncaa_weekly_rankings_schema,
)

ESPN_SCOREBOARD_URLS = {
    "nfl": "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard",
    "ncaaf": (
        "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard"
    ),
}

ESPN_RANKINGS_URL = (
    "https://site.api.espn.com/apis/site/v2/sports/football/college-football/rankings"
)

TABLE_CONFIGS = {
    "metadata": {
        "table_name": "game_metadata",
        "schema": game_metadata_schema,
        "key_columns": ["event_id"],
        "merge_condition": "T.event_id = S.event_id",
        "update_fields": ["sport_key", "sport_title", "commence_time", "home_team", "away_team"],
    },
    "scores": {
        "table_name": "game_results",
        "schema": game_score_schema,
        "key_columns": ["event_id"],
        "merge_condition": "T.event_id = S.event_id",
        "update_fields": [
            "sport_key",
            "sport_title",
            "commence_time",
            "completed",
            "last_update",
            "home_team",
            "away_team",
            "home_score",
            "away_score",
        ],
    },
    "rankings": {
        "table_name": "ncaa_weekly_rankings",
        "schema": ncaa_weekly_rankings_schema,
        "key_columns": ["weekNumber", "team"],
        "merge_condition": "T.weekNumber = S.weekNumber AND T.team = S.team",
        "update_fields": [
            "snapshot_time",
            "weekStartDate",
            "sport",
            "AP_Rank",
            "AP_Rank_LastWeek",
        ],
    },
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

    def query_ncaa_rankings(self) -> pd.DataFrame:
        """Fetch AP Top 25 NCAA football rankings from ESPN."""
        response = requests.get(ESPN_RANKINGS_URL, timeout=20)
        response.raise_for_status()
        payload = response.json() if isinstance(response.json(), dict) else {}

        rankings_list = payload.get("rankings", [])
        ap_poll = next((p for p in rankings_list if p.get("type") == "ap"), None)
        if not ap_poll:
            return pd.DataFrame(columns=[f.name for f in ncaa_weekly_rankings_schema])

        latest_week = payload.get("latestWeek", {})
        try:
            week_number = int(latest_week.get("number") or latest_week.get("value") or 1)
        except (TypeError, ValueError):
            week_number = 1

        poll_ranks = ap_poll.get("ranks", [])
        first_date = poll_ranks[0].get("date") if poll_ranks else None
        if first_date:
            try:
                week_start_date = pd.to_datetime(first_date).date()
            except Exception:
                week_start_date = datetime.date.today()
        else:
            week_start_date = datetime.date.today()

        snapshot_time = datetime.datetime.now(datetime.UTC)
        rows = []
        for rank_entry in poll_ranks:
            team_info = rank_entry.get("team", {})
            location = team_info.get("location", "").strip()
            nickname = team_info.get("name", "").strip()
            team_name = f"{location} {nickname}".strip() or team_info.get("displayName") or "Unknown"

            try:
                current_rank = int(rank_entry.get("current"))
            except (TypeError, ValueError):
                continue

            try:
                prev_rank = (
                    int(rank_entry.get("previous"))
                    if rank_entry.get("previous") is not None
                    else None
                )
            except (TypeError, ValueError):
                prev_rank = None

            rows.append(
                {
                    "snapshot_time": snapshot_time,
                    "weekNumber": week_number,
                    "weekStartDate": week_start_date,
                    "sport": "americanfootball_ncaaf",
                    "team": team_name,
                    "AP_Rank": current_rank,
                    "AP_Rank_LastWeek": prev_rank,
                }
            )

        return pd.DataFrame(rows, columns=[f.name for f in ncaa_weekly_rankings_schema])

    def upsert_to_bq(
        self,
        data: pd.DataFrame,
        table_type: str,
        project_id: str | None = None,
        dataset_id: str | None = None,
    ) -> int:
        """Upsert metadata, scores, or rankings dataframe into BigQuery using MERGE."""
        if table_type not in TABLE_CONFIGS:
            raise ValueError(
                f"Unsupported table_type: {table_type}. Choose from {list(TABLE_CONFIGS.keys())}"
            )

        if data.empty:
            return 0

        config = TABLE_CONFIGS[table_type]
        proj = project_id or GCP_PROJECT_ID
        ds = dataset_id or DATASET_ID
        target_table_id = f"{proj}.{ds}.{config['table_name']}"

        # Clean and coerce timestamp and date columns
        df = data.copy()
        for field in config["schema"]:
            if field.field_type == "TIMESTAMP" and field.name in df.columns:
                df[field.name] = pd.to_datetime(df[field.name], utc=True)
            elif field.field_type == "DATE" and field.name in df.columns:
                df[field.name] = pd.to_datetime(df[field.name]).dt.date

        # Deduplicate incoming batch by key columns keeping last occurrence
        key_cols = config.get("key_columns", ["event_id"])
        df = df.drop_duplicates(subset=key_cols, keep="last")

        client = bigquery.Client(project=proj, credentials=self.gcp_service_accnt)
        client.create_table(bigquery.Table(target_table_id, schema=config["schema"]), exists_ok=True)

        staging_suffix = f"_staging_{config['table_name']}_{int(datetime.datetime.now(datetime.UTC).timestamp())}"
        staging_table_id = f"{proj}.{ds}.{staging_suffix}"

        try:
            load_job = client.load_table_from_dataframe(
                df,
                staging_table_id,
                job_config=bigquery.LoadJobConfig(
                    schema=config["schema"],
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                ),
            )
            load_job.result()

            update_clause = ",\n    ".join(
                f"T.{f} = S.{f}" for f in config["update_fields"]
            )
            cols_clause = ", ".join(f.name for f in config["schema"])
            vals_clause = ", ".join(f"S.{f.name}" for f in config["schema"])
            merge_cond = config.get("merge_condition", "T.event_id = S.event_id")

            merge_sql = f"""
            MERGE `{target_table_id}` T
            USING `{staging_table_id}` S
            ON {merge_cond}
            WHEN MATCHED THEN
              UPDATE SET
                {update_clause}
            WHEN NOT MATCHED THEN
              INSERT ({cols_clause})
              VALUES ({vals_clause});
            """

            query_job = client.query(merge_sql)
            query_job.result()
            return query_job.num_dml_affected_rows if query_job.num_dml_affected_rows is not None else len(df)
        finally:
            client.delete_table(staging_table_id, not_found_ok=True)


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

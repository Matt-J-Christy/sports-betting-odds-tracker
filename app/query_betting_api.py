"""Query The Odds API for sportsbook market snapshots and store them to GCS or BigQuery."""

from __future__ import annotations

import datetime
import os
import sys
from typing import Any, Iterable

import pandas as pd
import requests
from google.cloud import bigquery, storage
from google.oauth2 import service_account

sys.path.append(os.path.join(os.path.dirname(__file__), '../'))

from app.config.config import DATASET_ID, GCP_PROJECT_ID, gcp_service_accnt, my_api_key, settings  # noqa: E402
from app.config.conferences import is_p4_ncaaf_game  # noqa: E402
from app.ddl.table_schemas import odds_schema  # noqa: E402

DEFAULT_SPORTS = tuple(settings.the_odds_api.sports)


class OddsQuery:
    """Fetch NFL and NCAA Football odds from The Odds API, flatten payloads, and persist."""

    def __init__(
        self,
        api_key: str,
        gcp_service_accnt: service_account.Credentials | None = None,
        bookmakers: list[str] | None = None,
    ) -> None:
        self.api_key = api_key
        self.base_url = settings.the_odds_api.base_url
        self.gcp_service_accnt = gcp_service_accnt
        self.bookmakers = [b.lower() for b in (bookmakers or settings.the_odds_api.bookmakers)]
        self.markets = settings.the_odds_api.markets
        self.odds_format = settings.the_odds_api.odds_format
        self.regions = settings.the_odds_api.regions
        self.last_quota_info: dict[str, Any] = {}

    @staticmethod
    def _normalize_price(value: Any) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _normalize_point(value: Any) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _normalize_event_rows(self, events: Iterable[dict]) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        snapshot_time = datetime.datetime.now(datetime.timezone.utc).isoformat()

        for event in events or []:
            if not isinstance(event, dict):
                continue

            game_id = event.get("id")
            sport = event.get("sport_key") or event.get("sport") or "americanfootball_nfl"
            away_team = event.get("away_team")
            home_team = event.get("home_team")
            commence_time = event.get("commence_time")

            # Power 4 filter: for NCAA Football, only keep games where at least
            # one team is from the ACC, SEC, Big Ten, Big 12, or Notre Dame.
            # NFL games are always retained without filtering.
            if sport == "americanfootball_ncaaf" and not is_p4_ncaaf_game(
                home_team or "", away_team or ""
            ):
                continue

            for bookmaker in event.get("bookmakers", []):
                if not isinstance(bookmaker, dict):
                    continue
                if str(bookmaker.get("key", "")).lower() not in self.bookmakers:
                    continue

                for market in bookmaker.get("markets", []):
                    if not isinstance(market, dict):
                        continue

                    market_type = market.get("key")
                    for outcome in market.get("outcomes", []):
                        if not isinstance(outcome, dict):
                            continue
                        rows.append(
                            {
                                "snapshot_time": snapshot_time,
                                "game_id": str(game_id),
                                "sport": sport,
                                "away_team": away_team,
                                "home_team": home_team,
                                "commence_time": commence_time,
                                "market_type": market_type,
                                "team_or_outcome": outcome.get("name"),
                                "price": self._normalize_price(outcome.get("price")),
                                "market_point": self._normalize_point(outcome.get("point")),
                            }
                        )

        columns = [
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
        return pd.DataFrame(rows, columns=columns)

    def get_new_odds(
        self,
        sports: str | Iterable[str] | None = None,
        test_mode: bool = False,
    ) -> pd.DataFrame:
        if sports is None:
            sport_keys = list(DEFAULT_SPORTS)
        elif isinstance(sports, str):
            sport_keys = [sports]
        else:
            sport_keys = list(sports)

        all_dfs: list[pd.DataFrame] = []

        for sport_key in sport_keys:
            url = f"{self.base_url}/v4/sports/{sport_key}/odds"
            params = {
                "apiKey": self.api_key,
                "regions": self.regions,
                "markets": self.markets,
                "oddsFormat": self.odds_format,
            }

            response = requests.get(url, params=params, timeout=30)
            self.last_quota_info[sport_key] = {
                "status_code": response.status_code,
                "requests_remaining": response.headers.get("x-requests-remaining"),
                "requests_used": response.headers.get("x-requests-used"),
            }
            print(
                f"[{sport_key}] Status: {response.status_code} | "
                f"Remaining Quota: {self.last_quota_info[sport_key]['requests_remaining']} | "
                f"Used: {self.last_quota_info[sport_key]['requests_used']}"
            )
            response.raise_for_status()
            events = response.json()

            if test_mode and events:
                events = events[:1]

            sport_df = self._normalize_event_rows(events)
            if not sport_df.empty:
                all_dfs.append(sport_df)

        if not all_dfs:
            return pd.DataFrame(
                columns=[
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
            )

        return pd.concat(all_dfs, ignore_index=True)

    def write_to_gcs(self, betting_data: pd.DataFrame) -> None:
        if betting_data.empty:
            print("No odds rows to write to GCS.")
            return

        now = datetime.datetime.now(datetime.timezone.utc)
        write_date = now.strftime("%Y-%m-%d")
        write_time = now.strftime("%H%M%S")

        bucket_name = settings.gcp.storage.bucket_name
        prefix = settings.gcp.storage.odds_prefix
        storage_client = storage.Client(credentials=self.gcp_service_accnt)
        bucket = storage_client.get_bucket(bucket_name)

        blob_path = f"{prefix}_{write_date}/{write_time}_data.csv"
        bucket.blob(blob_path).upload_from_string(
            betting_data.to_csv(index=False),
            content_type="text/csv",
        )

        print("Writing data to GCS location:", f"{bucket_name}/{blob_path}")
        print("Number of rows:", betting_data.shape[0])

    def write_to_bq(
        self,
        betting_data: pd.DataFrame,
        table_id: str | None = None,
    ) -> None:
        if betting_data.empty:
            print("No odds rows to write to BigQuery.")
            return

        target_table_id = table_id or settings.gcp.full_odds_table
        bq = bigquery.Client(credentials=self.gcp_service_accnt)

        job_config = bigquery.LoadJobConfig(
            schema=odds_schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        job = bq.load_table_from_dataframe(
            betting_data,
            target_table_id,
            job_config=job_config,
        )
        job.result()
        print(f"Successfully appended {betting_data.shape[0]} rows to BigQuery table {target_table_id}.")


if __name__ == "__main__":
    query = OddsQuery(api_key=my_api_key, gcp_service_accnt=gcp_service_accnt)
    bet_data = query.get_new_odds(test_mode=True)
    query.write_to_gcs(betting_data=bet_data)


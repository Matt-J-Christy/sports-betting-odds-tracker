"""Explore API responses without writing to GCS or BigQuery.

Examples:
    python scripts/explore_api_data.py --source fixture --kind odds
    python scripts/explore_api_data.py --source live --kind metadata --date 2026-09-13
    python scripts/explore_api_data.py --source fixture --kind scores --output /tmp/scores.csv
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.config.config import gcp_service_accnt, my_api_key  # noqa: E402
from app.query_betting_api import OddsQuery  # noqa: E402
from app.query_games_api import GamesQuery  # noqa: E402

FIXTURES = {
    "odds": ROOT / "app/tests/test_the_odds_api_parsing.py",
    "metadata": ROOT / "app/tests/games_sample_data.json",
    "scores": ROOT / "app/tests/games_sample_data.json",
}


def _load_fixture(kind: str) -> pd.DataFrame:
    if kind == "odds":
        source = FIXTURES[kind].read_text()
        module = ast.parse(source)
        assignment = next(
            node for node in module.body
            if isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "SAMPLE_EVENT"
                    for target in node.targets)
        )
        event = ast.literal_eval(assignment.value)
        return OddsQuery(api_key="fixture", gcp_service_accnt=None)._normalize_event_rows(
            [event]
        )

    payload = json.loads(FIXTURES[kind].read_text())
    frame = pd.json_normalize(payload.get("response", []))
    if kind == "metadata":
        columns = {
            "game.id": "game_id",
            "game.stage": "game_stage",
            "game.week": "week",
            "game.date.date": "game_date",
            "game.date.time": "game_time_utc",
            "game.venue.city": "city",
            "teams.home.id": "home_team_id",
            "teams.home.name": "home_team",
            "teams.away.id": "away_team_id",
            "teams.away.name": "away_team",
        }
    else:
        columns = {
            "game.id": "game_id",
            "teams.home.id": "home_team_id",
            "teams.away.id": "away_team_id",
            "scores.home.quarter_1": "home_score_q1",
            "scores.home.quarter_2": "home_score_q2",
            "scores.home.quarter_3": "home_score_q3",
            "scores.home.quarter_4": "home_score_q4",
            "scores.home.total": "home_score_final",
            "scores.away.quarter_1": "away_score_q1",
            "scores.away.quarter_2": "away_score_q2",
            "scores.away.quarter_3": "away_score_q3",
            "scores.away.quarter_4": "away_score_q4",
            "scores.away.total": "away_score_final",
        }
    return frame.rename(columns=columns).reindex(columns=list(columns.values()))


def _load_live(kind: str, requested_date: date | None) -> pd.DataFrame:
    if not my_api_key:
        raise SystemExit("Set THE_ODDS_API_KEY or API_KEY before using --source live.")
    if kind == "odds":
        return OddsQuery(api_key=my_api_key, gcp_service_accnt=None).get_new_odds(
            test_mode=False
        )
    query = GamesQuery(api_key=my_api_key, gcp_service_accnt=gcp_service_accnt)
    if kind == "metadata":
        return query.query_game_metadata(requested_date)
    return query.query_game_scores(requested_date)


def _report(frame: pd.DataFrame) -> None:
    print(f"Rows: {len(frame)} | Columns: {len(frame.columns)}")
    print("\nColumns and inferred dtypes:")
    print(frame.dtypes.to_string())
    print("\nMissing values:")
    print(frame.isna().sum().to_string())
    print("\nSample rows:")
    print(frame.head(10).to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=("fixture", "live"), default="fixture")
    parser.add_argument("--kind", choices=("odds", "metadata", "scores"), default="odds")
    parser.add_argument("--date", type=date.fromisoformat, help="API date in YYYY-MM-DD format")
    parser.add_argument("--output", type=Path, help="Optional path for normalized CSV output")
    args = parser.parse_args()

    frame = _load_fixture(args.kind) if args.source == "fixture" else _load_live(args.kind, args.date)
    _report(frame)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(args.output, index=False)
        print(f"\nWrote normalized data to {args.output}")


if __name__ == "__main__":
    main()

"""Inspect normalized data and optionally create its BigQuery table.

The default mode makes no network calls. It prints a schema-aligned report and
the CREATE TABLE statement. Add ``--create`` only after reviewing the result.

Examples:
    python scripts/preview_bigquery_tables.py --table odds --input /tmp/odds.csv
    python scripts/preview_bigquery_tables.py --table metadata --create
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.config.config import DATASET_ID, GCP_PROJECT_ID, gcp_service_accnt
from app.ddl.table_schemas import game_metadata_schema, game_score_schema, odds_schema


TABLES = {
    "odds": ("daily_odds_data", odds_schema),
    "metadata": ("game_metadata", game_metadata_schema),
    "scores": ("game_results", game_score_schema),
}


def _create_statement(table_id: str, schema: list) -> str:
    fields = ",\n".join(f"  `{field.name}` {field.field_type}" for field in schema)
    return f"CREATE TABLE IF NOT EXISTS `{table_id}` (\n{fields}\n);"


def _report(frame: pd.DataFrame, schema: list) -> None:
    expected = [field.name for field in schema]
    missing = [name for name in expected if name not in frame.columns]
    extra = [name for name in frame.columns if name not in expected]
    print(f"Rows: {len(frame)} | Columns: {len(frame.columns)}")
    print(f"Missing schema columns: {missing or 'none'}")
    print(f"Extra input columns: {extra or 'none'}")
    print("\nNull counts:")
    print(frame.reindex(columns=expected).isna().sum().to_string())
    print("\nInput dtypes:")
    print(frame.dtypes.to_string())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", choices=TABLES, required=True)
    parser.add_argument("--input", type=Path, help="Optional normalized CSV to inspect")
    parser.add_argument("--project", default=GCP_PROJECT_ID)
    parser.add_argument("--dataset", default=DATASET_ID)
    parser.add_argument("--create", action="store_true", help="Create the table in BigQuery")
    args = parser.parse_args()

    table_name, schema = TABLES[args.table]
    table_id = f"{args.project}.{args.dataset}.{table_name}"
    if args.input:
        frame = pd.read_csv(args.input)
        _report(frame, schema)
    print("\nBigQuery schema:")
    print(_create_statement(table_id, schema))

    if args.create:
        from google.cloud import bigquery

        client = bigquery.Client(project=args.project, credentials=gcp_service_accnt)
        table = bigquery.Table(table_id, schema=schema)
        created = client.create_table(table, exists_ok=True)
        print(f"\nBigQuery table ready: {created.full_table_id}")


if __name__ == "__main__":
    main()
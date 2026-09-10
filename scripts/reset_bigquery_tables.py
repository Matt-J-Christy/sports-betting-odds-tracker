"""Drop and recreate the configured BigQuery tables after explicit confirmation."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.config.config import gcp_service_accnt, settings  # noqa: E402
from app.ddl.table_schemas import game_metadata_schema, game_score_schema, odds_schema  # noqa: E402

TABLES = {
    settings.gcp.tables.odds_table: odds_schema,
    settings.gcp.tables.metadata_table: game_metadata_schema,
    settings.gcp.tables.scores_table: game_score_schema,
}


def reset_tables(project: str, dataset: str, confirmation: str, client=None) -> list[str]:
    expected_dataset = f"{settings.gcp.project_id}.{settings.gcp.dataset_id}"
    requested_dataset = f"{project}.{dataset}"
    if requested_dataset != expected_dataset:
        raise ValueError(f"Refusing unexpected dataset {requested_dataset}; expected {expected_dataset}")
    if confirmation != requested_dataset:
        raise ValueError(f"Type {requested_dataset} exactly to confirm the destructive reset")

    if client is None:
        from google.cloud import bigquery

        client = bigquery.Client(project=project, credentials=gcp_service_accnt)

    recreated: list[str] = []
    from google.cloud import bigquery

    for table_name, schema in TABLES.items():
        table_id = f"{project}.{dataset}.{table_name}"
        client.delete_table(table_id, not_found_ok=True)
        client.create_table(bigquery.Table(table_id, schema=schema))
        recreated.append(table_id)
    return recreated


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", default=settings.gcp.project_id)
    parser.add_argument("--dataset", default=settings.gcp.dataset_id)
    parser.add_argument(
        "--confirm",
        required=True,
        help="Must exactly equal PROJECT.DATASET; this operation deletes table data.",
    )
    args = parser.parse_args()
    for table_id in reset_tables(args.project, args.dataset, args.confirm):
        print(f"Recreated {table_id}")


if __name__ == "__main__":
    main()

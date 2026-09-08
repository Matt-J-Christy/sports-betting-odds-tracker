"""Create or replace the current FanDuel-style odds history table."""

from __future__ import annotations

import os
import sys

from google.cloud import bigquery

sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from app.config.config import DATASET_ID, GCP_PROJECT_ID, gcp_service_accnt  # noqa: E402

project = GCP_PROJECT_ID
dataset_id = DATASET_ID
table_id = "daily_odds_data"

bq = bigquery.Client(credentials=gcp_service_accnt)

sql = """
CREATE OR REPLACE TABLE `{}.{}.{}`
(
    snapshot_time TIMESTAMP,
    game_id STRING,
    sport STRING,
    away_team STRING,
    home_team STRING,
    commence_time TIMESTAMP,
    market_type STRING,
    team_or_outcome STRING,
    price INT64,
    market_point FLOAT64
)
""".format(project, dataset_id, table_id)

job = bq.query(sql)
job.result()

print(f'Created or replaced table "{project}.{dataset_id}.{table_id}".')

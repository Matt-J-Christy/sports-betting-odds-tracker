"""
DDL for game metadata table
and game results table
"""

import os
import sys

from google.cloud import bigquery

sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from app.config.config import gcp_service_accnt  # noqa: E402

project = 'odds-tracker-402301'
dataset_id = 'nfl_data'
table_id = 'game_metadata'


bq = bigquery.Client(credentials=gcp_service_accnt)


sql = f"""
create or replace table `{project}.{dataset_id}.{table_id}`
(
    game_id int64,
    game_stage string,
    week int,
    game_date date,
    game_time_utc string,
    city string,
    home_team_id int64,
    home_team string,
    away_team_id int64,
    away_team string
)
"""

job = bq.query(sql)  # API request.
job.result()  # Waits for the query to finish.

print(
    'Created or replaced table "{}.{}.{}".'.format(
        job.destination.project,  # type: ignore
        job.destination.dataset_id,  # type: ignore
        job.destination.table_id,  # type: ignore
    )
)

"""
Game scores table DDL
"""

from google.cloud import bigquery
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from app.config.config import gcp_service_accnt  # noqa: E402

project = 'odds-tracker-402301'
dataset_id = 'nfl_data'
table_id = 'game_results'


bq = bigquery.Client(credentials=gcp_service_accnt)


sql = """
create or replace table `{}.{}.{}`
(
    game_id int64,
    home_team_id int64,
    away_team_id int64,
    home_score_q1 int64,
    home_score_q2 int64,
    home_score_q3 int64,
    home_score_q4 int64,
    home_score_final int64,
    away_score_q1 int64,
    away_score_q2 int64,
    away_score_q3 int64,
    away_score_q4 int64,
    away_score_final int64
)
""".format(
    project, dataset_id, table_id
)

job = bq.query(sql)  # API request.
job.result()  # Waits for the query to finish.

print(
    'Created or replaced table "{}.{}.{}".'.format(
        job.destination.project,  # type: ignore
        job.destination.dataset_id,  # type: ignore
        job.destination.table_id,  # type: ignore
    )
)

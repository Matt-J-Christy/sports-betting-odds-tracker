"""
Python script to create or replace daily_odds_data table
"""

from google.cloud import bigquery
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))
from app.config.config import gcp_service_accnt  # noqa: E402

project = 'odds-tracker-402301'
dataset_id = 'nfl_data'
table_id = 'daily_odds_data'


bq = bigquery.Client(credentials=gcp_service_accnt)


sql = """
create or replace table `{}.{}.{}`
(
    value STRING,
    odd float64,
    bet_subgroup string,
    subgroup_value float64,
    bet_id int64,
    bet_name string,
    bookmaker_id int64,
    book string,
    game_id int64,
    update_date datetime
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

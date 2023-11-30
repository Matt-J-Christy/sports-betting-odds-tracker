"""
Backfilling data
"""

import pandas as pd
import time
from google.oauth2 import service_account
from google.cloud import storage, bigquery
import io
from typing import List
from app.config.config import my_api_key, gcp_service_accnt
from app.metadata_gcs_to_bq import GameMetadataToBq
from app.scores_gcs_to_bq import GameScoresToBq
from app.query_games_api import GamesQuery
from app.ddl.table_schemas import odds_schema
from app.helper_funcs import get_bets


query = GamesQuery(api_key=my_api_key,
                   gcp_service_accnt=gcp_service_accnt
                   )

gcs_to_bq = GameMetadataToBq(
    gcs_bucket='odds-data',
    table_id='odds-tracker-402301.nfl_data.game_metadata',
    gcp_service_accnt=gcp_service_accnt
)

scores_gcs_to_bq = GameScoresToBq(
    gcs_bucket='odds-data',
    table_id='odds-tracker-402301.nfl_data.game_results',
    gcp_service_accnt=gcp_service_accnt
)

# sundays = pd.date_range(start='2023-10-01',
#                         end='2023-11-07',
#                         freq='W-SUN').strftime('%Y-%m-%d').tolist()

# mondays = pd.date_range(start='2023-10-01',
#                         end='2023-11-07',
#                         freq='W-MON').strftime('%Y-%m-%d').tolist()


# dates = sundays + mondays

dates = ['2023-11-23', '2023-11-24']

# write data to GCS
for date in dates:
    print('getting data for', date)
    date = pd.to_datetime(date).date()
    try:
        score_data = query.query_game_scores(date=date)
        time.sleep(5)
        game_metadata = query.query_game_metadata(date=date)
        time.sleep(5)
        query.write_to_gcs(data=score_data, data_topic='scores', date=date)
        query.write_to_gcs(data=game_metadata,
                           data_topic='metadata', date=date)
    except:  # noqa
        print('backfill for {} failed'.format(date))


# write data to BQ
for date in dates:
    data = gcs_to_bq.get_latest_gcs_data(date=date)
    gcs_to_bq.write_to_bq(metadata_df=data)


# trying to backfill odds
game_ids = ['7696', '7697', '7694', '7695']


class BackfillBets():

    def __init__(
            self, api_key: str,
            gcp_service_accnt: service_account.Credentials,
            gcs_bucket: str,
            table_id: str) -> None:
        self.base_url = "https://v1.american-football.api-sports.io"
        self.headers = {
            'x-rapidapi-key': api_key,
            'x-rapidapi-host': 'v1.american-football.api-sports.io'
        }
        self.game_url = self.base_url + '/games'
        self.odds_url = self.base_url + '/odds'
        self.gcp_service_accnt = gcp_service_accnt
        self.gcs_bucket = gcs_bucket
        self.table_id = table_id

    def get_new_odds(self, game_ids: List[str]) -> pd.DataFrame:
        bet_ids = [1, 3, 4, 30, 6]

        all_bets = get_bets(
            url=self.odds_url,
            headers=self.headers,
            game_ids=game_ids,
            bet_ids=bet_ids

        )

        return all_bets

    def write_to_gcs(self, betting_data: pd.DataFrame, date: str) -> None:

        storage_client = storage.Client(credentials=self.gcp_service_accnt)
        bucket = storage_client.get_bucket('odds-data')

        bucket\
            .blob('betting_odds_' + date + '/backfill_data.csv')\
            .upload_from_string(betting_data.to_csv(index=False), 'text/csv')

        print('Writing data to GCS location:',
              'odds-data/bettings_odds_' + date)
        print('Number of rows:', betting_data.shape[0])

    def from_gcs_to_bq(self, date: str) -> None:
        """
        Write BQ table and insert values
        """

        storage_client = storage.Client(credentials=self.gcp_service_accnt)
        bq = bigquery.Client(credentials=self.gcp_service_accnt)
        bucket = storage_client.get_bucket(self.gcs_bucket)

        blob_name = 'betting_odds_' + date + '/backfill_data.csv'
        new_blob = bucket.blob(blob_name=blob_name)
        blob_bytes = new_blob.download_as_bytes()

        df = pd.read_csv(io.BytesIO(blob_bytes))
        df['update_date'] = pd.to_datetime(df['update_date'])

        job_config = bigquery.LoadJobConfig(
            schema=odds_schema
        )

        job = bq.load_table_from_dataframe(
            df,
            self.table_id,
            job_config=job_config
        )

        print(job.result)
        print('Writing {} rows to table'.format(df.shape[0]))


odds_query = BackfillBets(
    api_key=my_api_key,
    gcp_service_accnt=gcp_service_accnt,
    gcs_bucket='odds-data',
    table_id='odds-tracker-402301.nfl_data.daily_odds_data'
)

new_odds = odds_query.get_new_odds(game_ids=game_ids)


odds_query.write_to_gcs(betting_data=new_odds, date='2023-11-29')

odds_query.from_gcs_to_bq(date='2023-11-29')

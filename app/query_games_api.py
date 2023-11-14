"""
File to query games API for metadata
"""


import os
import sys
import datetime
from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
from typing import Optional

sys.path.append(os.path.join(os.path.dirname(__file__), '../'))

from app.config.config import my_api_key, gcp_service_accnt  # noqa: E402
from app.helper_funcs import get_game_metadata, get_game_scores  # noqa: E402


class GamesQuery():
    """
    Class used to get NFL game metadata and the results of past game scores.
    Write resulting dataframes to GCS bucket
    """

    def __init__(self, api_key: str,
                 gcp_service_accnt: service_account.Credentials) -> None:
        self.base_url = "https://v1.american-football.api-sports.io"
        self.headers = {
            'x-rapidapi-key': api_key,
            'x-rapidapi-host': 'v1.american-football.api-sports.io'
        }
        self.game_url = self.base_url + '/games'
        self.gcp_service_accnt = gcp_service_accnt

    def query_game_metadata(self, date: Optional[datetime.date] = None) -> pd.DataFrame:

        if date is None:
            today = datetime.date.today()
            # next_thursday = today + datetime.timedelta(3 - today.weekday() % 7)
            # apparently the API doesn't have Thursday night football data
            next_sunday = today + datetime.timedelta((6 - today.weekday() % 7))
            next_monday = today + datetime.timedelta((7 - today.weekday() % 7))

            dates = [next_sunday, next_monday]

        else:
            dates = [date]

        df = pd.DataFrame()

        for date in dates:
            res = get_game_metadata(
                game_url=self.game_url,
                headers=self.headers,
                date=date
            )

            df = pd.concat([df, res], axis=0)

        return df

    def query_game_scores(self, date: Optional[datetime.date] = None) -> pd.DataFrame:

        if date is None:
            today = datetime.date.today()
            # next_thursday = today + datetime.timedelta(3 - today.weekday() % 7)
            # API does not have Thursday data
            next_sunday = today + datetime.timedelta((6 - today.weekday() % 7))
            next_monday = today + datetime.timedelta((7 - today.weekday() % 7))

            dates = [next_sunday, next_monday]
        else:
            dates = [date]

        df = pd.DataFrame()

        for date in dates:
            res = get_game_scores(
                game_url=self.game_url,
                headers=self.headers,
                date=date
            )

            df = pd.concat([df, res], axis=0)

        return df

    def write_to_gcs(self,
                     data: pd.DataFrame,
                     data_topic: str,
                     date: Optional[datetime.date] = None):

        if date is None:
            today = datetime.date.today()
            write_date = str(today)
        else:
            write_date = str(date)

        storage_client = storage.Client(credentials=self.gcp_service_accnt)
        bucket = storage_client.get_bucket('odds-data')

        name = ''

        if data_topic == 'metadata':
            name = '/game_metadata.csv'

        elif data_topic == 'scores':
            name = '/score_data.csv'

        else:
            print('invalid data topic given')

        bucket\
            .blob('game_data_' + write_date + name)\
            .upload_from_string(data.to_csv(index=False), 'text/csv')

        print('Writing data to GCS location:', 'odds-data/game_data' +
              write_date + name)
        print('Number of rows:', data.shape[0])


if __name__ == '__main__':
    query = GamesQuery(api_key=my_api_key, gcp_service_accnt=gcp_service_accnt)
    score_data = query.query_game_scores()
    game_metadata = query.query_game_metadata()

    query.write_to_gcs(data=score_data, data_topic='scores')
    query.write_to_gcs(data=game_metadata, data_topic='metadata')

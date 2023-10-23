"""
Query betting odds API and write results to Google cloud storage 
"""


import os
import sys
import datetime
from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), '../'))

from app.config.config import my_api_key, gcp_service_accnt
from app.helper_funcs import get_game_ids, get_bets


class OddsQuery():
    """
    Querying Odds and Games API: 
    - base URL: https://v1.american-football.api-sports.io
    - games 
        - need an Id for each game in order to query the odds endpoint
        - Games API can get game information weeks into the future
    - bookmaker
        - bet 365 is the bookmaker I'm choosing
        it looks like the API only has EU books
        - Odds API can get bets one week into the future
    - bet id types
        - 1: moneyline win odds
        - 3: over/under
        - 4: over/under 1st half
        - 30: over/under 2nd half
        - 6: Handicap result (spread bet)
    - JSON structure is highly nested based on book, bet type and values of the bet
    - created "test mode" parameter to limit the number of API calls when working
    on the scripts. Free tier of the API has 100 calls per day. 

    API error messages: 
    Response code: 
    - 200 --> success
    - 401 --> unauthorized
    - 404 --> not found
    - 500 --> internal server error
    """

    def __init__(
        self, api_key: str,
        gcp_service_accnt: service_account.Credentials
    ) -> None:
        self.base_url = "https://v1.american-football.api-sports.io"
        self.headers = {
            'x-rapidapi-key': api_key,
            'x-rapidapi-host': 'v1.american-football.api-sports.io'
        }
        self.game_url = self.base_url + '/games'
        self.odds_url = self.base_url + '/odds'
        self.gcp_service_accnt = gcp_service_accnt

    def get_new_odds(self, test_mode: bool) -> pd.DataFrame:

        game_url = self.game_url
        odds_url = self.odds_url
        headers = self.headers

        today = datetime.date.today()
        next_sunday = today + datetime.timedelta((6 - today.weekday() % 7))
        next_monday = today + datetime.timedelta((7 - today.weekday() % 7))

        dates = [next_sunday, next_monday]
        all_game_ids = []

        if test_mode:
            dates = [dates[0]]

        for i in dates:

            print('Getting game ids for date:', str(i))

            ids = get_game_ids(
                game_url=game_url,
                headers=headers,
                date=i
            )
            all_game_ids = all_game_ids + ids

        if test_mode:
            all_game_ids = [all_game_ids[0]]

        print(all_game_ids)

        bet_ids = [1, 3, 4, 30, 6]

        all_bets = get_bets(
            url=odds_url,
            headers=headers,
            game_ids=all_game_ids,
            bet_ids=bet_ids
        )

        return all_bets

    def write_to_gcs(self, betting_data: pd.DataFrame) -> None:
        today = datetime.date.today()
        write_date = str(today)

        storage_client = storage.Client(credentials=self.gcp_service_accnt)
        bucket = storage_client.get_bucket('odds-data')

        bucket\
            .blob('bettings_odds_' + write_date + '/data.csv')\
            .upload_from_string(betting_data.to_csv(index=False), 'text/csv')
        
        print('Writing data to GCS location:', 'odds-data/bettings_odds_' + write_date)
        print('Number of rows:', betting_data.shape[0])

if __name__ == '__main__':
    query = OddsQuery(api_key=my_api_key, gcp_service_accnt=gcp_service_accnt)
    bet_data = query.get_new_odds(test_mode=True)
    query.write_to_gcs(betting_data=bet_data)

"""
Utility functions for querying API

- get_game_ids grabs the relevant game ids for the current NFL season
- normalize_odds_json formats the response for the odds API into
a pandas dataframe
- get_bets is the function for querying the odds API and returns a dataframe
"""

import json
import time
from typing import Dict, List
import datetime
import requests
import numpy as np
import pandas as pd


def get_game_ids(game_url: str,
                 headers: Dict[str, str],
                 date: datetime.date) -> List[int]:
    """
    Get relevant NFL odds for the current year
    """

    year = datetime.date.today().year

    payload = {
        "league": "1",
        "season": str(year),
        "date": str(date)
    }

    games_res = requests.request(
        "GET", game_url, headers=headers, params=payload, timeout=30)

    json_result = json.loads(games_res.text)

    game_ids = []

    for i in range(len(json_result['response'])):
        game_ids.append(json_result['response'][i]['game']['id'])

    return game_ids


def normalize_odds_json(data: Dict[str, str], bet_ids: List[int]) -> pd.DataFrame:
    """
    Formats JSON response from the betting API
    """

    odds_results = pd.json_normalize(
        data=data,
        record_path=['bookmakers', 'bets', 'values'],
        meta=[
            ['bookmakers', 'bets', 'id'],
            ['bookmakers', 'bets', 'name'],
            ['bookmakers', 'id'],
            ['bookmakers', 'name']
        ]
    )\
        .rename(columns={
            'bookmakers.bets.id': 'bet_id',
            'bookmakers.id': 'bookmaker_id',
            'bookmakers.name': 'book',
            'bookmakers.bets.name': 'bet_name'
        })

    odds_results = odds_results\
        .loc[odds_results['bet_id'].isin(bet_ids), :]\
        .reset_index(drop=True)

    return odds_results


def get_bets(url: str,
             headers: Dict[str, str],
             game_ids: List[str],
             bet_ids: List[int]) -> pd.DataFrame:
    """
    Function to query the odds api, format the data
    and filter the resulting dataframe. The odds API
    returns multiple alternate lines for a bet id - subgroup type, so I
    pick the closes bets to 2, aka "even odds"

    Ex: A set of over/under bets could be
    {(30, 29.5), {31, 30.5}, ... (45, 44)}
    """

    result_df = pd.DataFrame()

    # iterate over game ids to get the bets
    for game in game_ids:
        print('getting bets for game:', game)

        payload = {
            "game": game,
            # bet 365 (looks like this API does not pull american odds makers)
            "bookmaker": 4,
        }

        response = requests.request(
            "GET", url, headers=headers, params=payload, timeout=30)

        json_result = json.loads(response.text)

        # pausing the API for 3 seconds so the API calls aren't throttled
        time.sleep(3)

        if json_result['response'] == []:
            print('Empty bet result returned for game:', game)

        else:
            bet_df = normalize_odds_json(
                data=json_result['response'][0], bet_ids=bet_ids)
            bet_df['game_id'] = game
            bet_df['update_date'] = datetime.datetime\
                                            .now()\
                                            .strftime('%Y-%m-%d %H:%M:%S')

            bet_df['dist_from_even'] = np.abs(bet_df['odd'].astype(float) - 2)
            bet_df['subgroup_value'] = bet_df['value'].str.extract(
                r'([-+]?\d*\.?\d+)')
            bet_df['bet_subgroup'] = bet_df['value'].str.extract(
                r"([a-zA-Z]+)")
            bet_df['odds_rank'] = bet_df\
                .groupby(['game_id', 'bet_id', 'bet_subgroup'])['dist_from_even']\
                .rank(ascending=True, method='first')

            bet_df = bet_df.loc[bet_df['odds_rank'] <= 2, :]

            result_df = pd.concat([result_df, bet_df], axis=0)

    return result_df.drop(['odds_rank', 'dist_from_even'], axis=1)

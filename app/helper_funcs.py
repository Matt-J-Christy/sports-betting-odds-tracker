# Utility functions

import requests
import json
import datetime
import pandas as pd
from typing import Dict, List
import time


def get_game_ids(game_url: str, headers: Dict[str, str], date: datetime.date) -> List[int]:

    payload = {
        "league": "1",
        "season": "2023",
        "date": str(date)
    }

    games_res = requests.request(
        "GET", game_url, headers=headers, params=payload)

    json_result = json.loads(games_res.text)

    game_ids = []

    for i in range(len(json_result['response'])):
        game_ids.append(json_result['response'][i]['game']['id'])

    return game_ids


def normalize_odds_json(data: Dict[str, str], bet_ids: List[int]) -> pd.DataFrame:
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


def get_bets(url: str, headers: Dict[str, str], game_ids: List[str], bet_ids: List[int]) -> pd.DataFrame:

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
            "GET", url, headers=headers, params=payload)

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
            bet_df = bet_df.loc[
                            (bet_df['bet_name'] != 'Over/Under') | 
                            ((bet_df['odd'].astype(float) < 2) &
                            (bet_df['odd'].astype(float) > 1.9)),
                            :]
            
            result_df = pd.concat([result_df, bet_df], axis=0)

    return result_df
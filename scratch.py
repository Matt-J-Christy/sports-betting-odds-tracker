"""
Response code: 
- 200 --> success
- 401 --> unauthorized
- 404 --> not found
- 500 --> internal server error
"""

import requests
import json
from app.config.config import my_api_key
import datetime
import pandas as pd
from typing import Dict, List
import time


base_url = "https://v1.american-football.api-sports.io"

headers = {
    'x-rapidapi-key': my_api_key,
    'x-rapidapi-host': 'v1.american-football.api-sports.io'
}

# get games
game_url = base_url + '/games'

today = datetime.date.today()
next_sunday = today + datetime.timedelta((6 - today.weekday() % 7))
next_monday = today + datetime.timedelta((7 - today.weekday() % 7))

dates = [next_sunday]


def get_game_ids(base_url: str, headers: Dict[str, str], date: datetime.date) -> List[int]:
    game_url = base_url + '/games'

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


all_game_ids = []

for i in dates:

    print('Getting game ids for date:', str(i))

    ids = get_game_ids(
        base_url=base_url,
        headers=headers,
        date=i
    )
    all_game_ids = all_game_ids + ids

print(all_game_ids)

"""
Notes: 

- games 
    - need an Id for each game

- bookmaker
    - bet 365 is the bookmaker I'm choosing
    it looks like the API only has EU books

- bet id types
    - 1: moneyline win odds
    - 3: over/under
    - 4: over/under 1st half
    - 30: over/under 2nd half
    - 6: Handicap result (spread bet)

- JSON structure is highly nested based on book, bet type and values of the bet

- Games API can get game information weeks into the future
- Odds API can get bets one week into the future
"""

odds_url = base_url + '/odds'

bet_ids = [1, 3, 4, 30, 6]


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


def get_bets(url: str, headers: Dict[str, str], game_ids: List[str]) -> pd.DataFrame:

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


all_bets = get_bets(
    url=odds_url,
    headers=headers,
    game_ids=all_game_ids
)


all_bets.shape

all_bets.head()

all_bets.groupby(['game_id', 'bet_name']).count()


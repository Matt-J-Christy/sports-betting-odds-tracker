"""
Response code: 
- 200 --> success
- 401 --> unauthorized
- 404 --> not found
- 500 --> internal server error
"""

import requests
import json
from config import my_api_key
import datetime
import pandas as pd

base_url = "https://v1.american-football.api-sports.io"

headers = {
  'x-rapidapi-key': my_api_key,
  'x-rapidapi-host': 'v1.american-football.api-sports.io'
}

# get games 
game_url = base_url + '/games'

today = datetime.date.today()
idx = (today.weekday() + 1) % 7
last_sunday = today - datetime.timedelta(idx)
last_monday = today - datetime.timedelta((today.weekday() % 7))
next_sunday = today + datetime.timedelta((6 - today.weekday() % 7))
next_monday = today + datetime.timedelta((7 - today.weekday() % 7))

dates = [last_monday, last_sunday, next_sunday, next_monday]


def get_game_ids(base_url, headers, date):
    game_url = base_url + '/games'

    payload = {
        "league": "1",
        "season": "2023",
        "date": str(date)
        }

    games_res = requests.request("GET", game_url, headers=headers, params=payload)

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


# get odds for a given game, bet and book
# TODO: find the code for Fanduel or DK

odds_url = base_url + '/odds'

bet_ids = [1, 3, 4, 30, 6]

payload={
    "game": all_game_ids[0], 
    "bookmaker":4, # bet 365 (looks like this API does not pull american odds makers)
    #"bet": 1
}

response = requests.request("GET", odds_url, headers=headers, params=payload)

print(response.text)


json_result = json.loads(response.text)

data = json_result['response'][0]

data['bookmakers'][0]['bets'][1]

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



odds_results.groupby('bet_name').count()
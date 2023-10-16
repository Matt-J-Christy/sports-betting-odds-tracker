"""
Test if the games API endpoint is working 
"""

import requests
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__),'../../'))

from app.config.config import my_api_key

base_url = "https://v1.american-football.api-sports.io"

headers = {
    'x-rapidapi-key': my_api_key,
    'x-rapidapi-host': 'v1.american-football.api-sports.io'
}

# get odds for a given game, bet and book

odds_url = base_url + '/odds'

payload = {
    "game": 7597,
    # bet 365 (looks like this API does not pull american odds makers)
    "bookmaker": 4,
    "bet": 1  # requesting just the results for the moneyline odds
}


res = requests.request("GET", odds_url, headers=headers, params=payload)


def test_api():
    assert res.status_code == 200, "Games API request failed"

if __name__ == '__main__':
    test_api()

    print('Bets endpoint test passed')
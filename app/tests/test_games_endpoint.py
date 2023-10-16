"""
Test if the games API endpoint is working 
"""

import requests
import datetime
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__),'../../'))

from app.config.config import my_api_key


base_url = "https://v1.american-football.api-sports.io"

headers = {
    'x-rapidapi-key': my_api_key,
    'x-rapidapi-host': 'v1.american-football.api-sports.io'
}

# get games
today = datetime.date.today()
idx = (today.weekday() + 1) % 7
last_sunday = today - datetime.timedelta(idx)


game_url = base_url + '/games'


def games_api_call(url, headers, date):

    payload = {
        "league": "1",
        "season": "2023",
        "date": str(date)
    }

    res = requests.request("GET", url, headers=headers, params=payload)

    return res


def test_api():
    res = games_api_call(
        url=game_url,
        headers=headers,
        date=last_sunday
    )

    assert res.status_code == 200, "Games API request failed"


if __name__ == '__main__':
    test_api()

    print('Games endpoint test passed')

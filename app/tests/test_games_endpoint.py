"""Unit tests for the games endpoint query helper."""

import datetime
from unittest.mock import MagicMock, patch

from app.config.config import my_api_key

base_url = "https://v1.american-football.api-sports.io"
game_url = base_url + "/games"
headers = {
    "x-rapidapi-key": my_api_key or "test_key",
    "x-rapidapi-host": "v1.american-football.api-sports.io",
}


def games_api_call(url, headers, date):
    import requests

    payload = {"league": "1", "season": "2023", "date": str(date)}
    return requests.request("GET", url, headers=headers, params=payload)


def test_games_api_call_mocked():
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"response": []}

    with patch("requests.request", return_value=mock_resp) as mock_req:
        target_date = datetime.date(2023, 9, 10)
        res = games_api_call(game_url, headers, target_date)

        assert res.status_code == 200
        assert mock_req.called
        call_args, call_kwargs = mock_req.call_args
        assert call_kwargs["params"]["league"] == "1"
        assert call_kwargs["params"]["date"] == "2023-09-10"

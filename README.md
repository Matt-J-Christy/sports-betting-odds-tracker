# Sports Betting Odds Tracker

## App Setup 

[API Docs](https://dashboard.api-football.com/)

```
mkdir odds_trackers
python -m venv odds-tracker
odds-tracker\Scripts\activate.bat
pip install -r requirements.in
```

### Relevat API Endpoints 

- Games endpoint `'https://v1.american-football.api-sports.io/games'`
    - Need to use `league_id` and `season year` and `date` to get the game ids for a given NFL week
- Odds endpoint `'https://v1.american-football.api-sports.io/odds`
    - Need to use `bookmaker` and `game id` to select the various wagers on the game
    - Can use `bet id` to select for specific types of bets

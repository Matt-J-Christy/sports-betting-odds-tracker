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


### Using the CLI

From your terminal you can query the odds API and write data to BigQuery. Below are example commands

- Command: `python cli.py get-new-odds` queries the NFL football API.
    - Option to run in `test-mode` to limit the number of API calls to 1 call.
- Command: `python cli.py write-odds-to-bq` moves odds data from GCS to BigQuery.
    - Option to specify a `--date` parameter in order to backfill data.
    - If `--date` is unused the function defaults to writing the most recent GCS data to BigQuery

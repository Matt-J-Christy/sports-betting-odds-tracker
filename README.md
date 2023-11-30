# Sports Betting Odds Tracker

### App Setup

```
mkdir odds_trackers
python -m venv odds-tracker
odds-tracker\Scripts\activate.bat
pip install -r requirements.in
```

### Relevat API Endpoints

[API Docs](https://dashboard.api-football.com/)

- Games endpoint `'https://v1.american-football.api-sports.io/games'`
    - Need to use `league_id` and `season year` and `date` to get the game ids for a given NFL week
    - Games endpoint is used to collect game metadata and the game scores
- Odds endpoint `'https://v1.american-football.api-sports.io/odds`
    - Need to use `bookmaker` and `game id` to select the various wagers on the game
    - Can use `bet id` to select for specific types of bets
    - Odds update at most 4 times a day and are stored for up to a week in the past

### Using the CLI

From your terminal you can query the odds API, Games API and write data to BigQuery. Below is an example on running the CLI.

`python cli.py get-new-odds test-mode=True`

Getting Odds Data
- Command: `get-new-odds` queries the NFL football API.
    - Option to run in `test-mode` to use only a single game, thus limiting the
    number of API calls.
- Command: `write-odds-to-bq` moves odds data from GCS to BigQuery.
    - Option to specify a `--date` parameter in order to backfill data.
    - If `--date` is unused the function defaults to writing the most recent GCS data to BigQuery

Getting Game Metadata
- Command: `get-game-metadata`
    - Option to specify `--date` for a specific day of games
- Command: `game-metadata-to-bq`
    - Option to specify a `--date` parameter in order to backfill data.
    - If `--date` is unused the function defaults to writing the most recent GCS data to BigQuery

Getting Game Results
- Command: `get-game-scores`
    - Option to specify `--date` for a specific day of games
- Command: `game-scores-to-bq`
    - Option to specify a `--date` parameter in order to backfill data.
    - If `--date` is unused the function defaults to writing the most recent GCS data to BigQuery

### Docker ETLs

Two shell scripts are used to run the CLI inside two docker containers. `run_cli.sh` collects daily odds and game metadata, while `game_scores.sh` is used to collect the past week's NFL scores.

Both docker images are deployed on GCP cloud run


### Streamlit App

The next phase of the project is to
create a streamlit app to track odds movements and game outcomes

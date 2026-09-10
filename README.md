# Sports Betting Odds Tracker

### App Setup

```
mkdir odds_trackers
python -m venv odds-tracker
odds-tracker\Scripts\activate.bat
pip install -r requirements.in
```

### Relevant API Endpoints

[ESPN scoreboard endpoints](https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard)

- ESPN supplies game metadata and scores without an API key for NFL and college football.
- The Odds API supplies sportsbook snapshots for `daily_odds_data`.
- Metadata and scores use ESPN event IDs (`event_id`) and final team scores; API-Sports fields and quarter-score columns are no longer part of the contract.

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
    - Queries ESPN and writes provider-native event metadata directly to BigQuery.
    - Optionally specify `--date YYYY-MM-DD` for a scoreboard date.

Getting Game Results
- Command: `get-game-scores`
    - Queries ESPN and writes provider-native final/current scores directly to BigQuery.
    - Optionally specify `--date YYYY-MM-DD` for a scoreboard date.

### Docker ETLs

The CLI and `run_cli.sh` collect odds from The Odds API and metadata/scores from ESPN. `game_scores.sh` remains available for scheduled score collection.

Both docker images are deployed on GCP cloud run


### Streamlit App

The next phase of the project is to
create a streamlit app to track odds movements and game outcomes

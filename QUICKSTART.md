# Quickstart Guide: Sports Betting Odds Tracker

This guide helps Data Scientists and Engineers quickly understand the BigQuery data models, generate data using the CLI, and start analyzing odds movements or building predictive models.

---

## 1. BigQuery Datasets & Data Models

All production tables reside in the dataset:
```text
odds-tracker-402301.nfl_data
```

### Table 1: `daily_odds_data`
Automated time-series snapshots of FanDuel sportsbook lines recorded every 4 hours. Each row represents a single outcome for a given market (e.g., moneyline on home team, over on total points, or spread on away team).

| Field Name | Type | Description | Example |
| :--- | :--- | :--- | :--- |
| `snapshot_time` | `TIMESTAMP` | UTC timestamp when the odds snapshot was retrieved | `2026-09-12 16:00:54 UTC` |
| `game_id` | `STRING` | Unique 32-character hexadecimal identifier from The Odds API | `8d0f17e3f22c60e...` |
| `sport` | `STRING` | Sport key: `americanfootball_nfl` or `americanfootball_ncaaf` | `americanfootball_nfl` |
| `away_team` | `STRING` | Name of visiting team | `San Francisco 49ers` |
| `home_team` | `STRING` | Name of host team | `Los Angeles Rams` |
| `commence_time` | `TIMESTAMP` | Scheduled kickoff time in UTC | `2026-09-11 00:35:00 UTC` |
| `market_type` | `STRING` | Betting market: `h2h` (moneyline), `spreads`, or `totals` | `spreads` |
| `team_or_outcome` | `STRING` | Team name (for spreads/moneyline) or `Over` / `Under` (totals) | `San Francisco 49ers` |
| `price` | `INT64` | American odds payout format (e.g., `-110`, `+150`) | `-110` |
| `market_point` | `FLOAT64` | Point spread handicap (e.g., `-3.5`) or total point line (e.g., `45.5`) | `-3.5` |

> 💡 **Tip**: To track line movements over time for a specific game, group by `game_id`, `market_type`, and `team_or_outcome`, and order by `snapshot_time ASC`.

---

### Table 2: `game_metadata`
Master event schedule and team registry retrieved from the ESPN Scoreboard API.

| Field Name | Type | Description | Example |
| :--- | :--- | :--- | :--- |
| `event_id` | `STRING` | Canonical ESPN event identifier | `401872657` |
| `sport_key` | `STRING` | Sport identifier: `espn_nfl` or `espn_ncaaf` | `espn_nfl` |
| `sport_title` | `STRING` | Display league name: `NFL` or `NCAA Football` | `NFL` |
| `commence_time` | `TIMESTAMP` | Scheduled game start time in UTC | `2026-09-11 00:35:00 UTC` |
| `home_team` | `STRING` | Canonical home team name from ESPN | `Los Angeles Rams` |
| `away_team` | `STRING` | Canonical away team name from ESPN | `San Francisco 49ers` |

---

### Table 3: `game_results`
Game outcomes, scores, and completion status retrieved from the ESPN Scoreboard API.

| Field Name | Type | Description | Example |
| :--- | :--- | :--- | :--- |
| `event_id` | `STRING` | Canonical ESPN event identifier | `401872657` |
| `sport_key` | `STRING` | Sport identifier: `espn_nfl` or `espn_ncaaf` | `espn_nfl` |
| `sport_title` | `STRING` | Display league name: `NFL` or `NCAA Football` | `NFL` |
| `commence_time` | `TIMESTAMP` | Scheduled kickoff time in UTC | `2026-09-11 00:35:00 UTC` |
| `completed` | `BOOL` | `True` if the game is final; `False` if upcoming or live | `True` |
| `last_update` | `TIMESTAMP` | Timestamp of last provider score update | `NULL` |
| `home_team` | `STRING` | Name of host team | `Los Angeles Rams` |
| `away_team` | `STRING` | Name of visiting team | `San Francisco 49ers` |
| `home_score` | `INT64` | Final or current score for the host team | `7` |
| `away_score` | `INT64` | Final or current score for the visiting team | `27` |

---

## 2. Using the CLI to Ingest & Generate Data

The project provides a unified CLI in `cli.py` for fetching live data and loading it into BigQuery.

### CLI Syntax Overview

```bash
python cli.py [COMMAND] [OPTIONS]
```

### Command Reference

#### 1. Ingest Betting Odds (`get-new-odds`)
Queries The Odds API for current FanDuel lines and writes snapshots directly to BigQuery and/or Cloud Storage.

```bash
# General syntax
python cli.py get-new-odds [OPTIONS]

# Options:
#   --sport [all|nfl|ncaaf]      Sport to collect (default: all)
#   --destination [bq|gcs|both]  Destination sink (default: bq)
#   --test-mode                  Fetch only 1 event to conserve API quota
```

**Examples:**
```bash
# Safe test run: fetch 1 game and verify pipeline without burning quota
python cli.py get-new-odds --sport all --test-mode

# Production pull: fetch all active NFL and NCAAF Power 4 games into BigQuery
python cli.py get-new-odds --sport all --destination bq

# Archive pull: save raw odds snapshot to both BigQuery and GCS
python cli.py get-new-odds --sport nfl --destination both
```

#### 2. Ingest Game Metadata (`get-game-metadata`)
Queries the ESPN Scoreboard API for upcoming or historical games and appends them to `game_metadata`.

```bash
# Query active/current scoreboard games
python cli.py get-game-metadata

# Query games for a specific calendar date (YYYY-MM-DD)
python cli.py get-game-metadata --date 2026-09-12
```

#### 3. Ingest Game Scores & Results (`get-game-scores`)
Queries the ESPN Scoreboard API for live scores or final results and appends them to `game_results`.

```bash
# Query active scoreboard scores
python cli.py get-game-scores

# Query scores for a specific calendar date
python cli.py get-game-scores --date 2026-09-12
```

#### 4. Backfill BigQuery from Cloud Storage (`write-odds-to-bq`)
Loads previously archived odds files from GCS bucket `odds-data` into BigQuery.

```bash
# Backfill latest GCS snapshot
python cli.py write-odds-to-bq

# Backfill a specific date
python cli.py write-odds-to-bq --date 2026-09-10
```

---

## 3. Querying BigQuery in Python (Data Science Workflow)

Here is a quick starter snippet for pulling data into a pandas DataFrame:

```python
from google.cloud import bigquery
import pandas as pd

client = bigquery.Client(project="odds-tracker-402301")

# Fetch recent spread movement for NFL matchups
query = """
SELECT 
    snapshot_time,
    away_team,
    home_team,
    team_or_outcome,
    market_point AS spread,
    price AS spread_odds
FROM `odds-tracker-402301.nfl_data.daily_odds_data`
WHERE sport = 'americanfootball_nfl' 
  AND market_type = 'spreads'
ORDER BY snapshot_time DESC
LIMIT 100
"""

df = client.query(query).to_dataframe()
print(df.head())
```

---

## 4. Local App & Testing

### Running Tests
Before submitting pull requests or schema updates, run the test suite:

```bash
pytest
```

### Running Lint Checks
Code quality and formatting are governed by Ruff:

```bash
ruff check .
```

### Launching the Streamlit Visualizer
Explore line movements interactively:

```bash
streamlit run streamlit/streamlit_app.py
```


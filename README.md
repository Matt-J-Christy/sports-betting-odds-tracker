# Sports Betting Odds Tracker

[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![Cloud Run: Deployed](https://img.shields.io/badge/GCP-Cloud%20Run-brightgreen.svg)](https://cloud.google.com/run)
[![BigQuery: Managed](https://img.shields.io/badge/GCP-BigQuery-blue.svg)](https://cloud.google.com/bigquery)

An automated data pipeline and analytics platform designed to collect, normalize, and store sports betting odds snapshots and game outcomes for the **NFL** and **NCAA Football (FBS Power 4)**. 

The pipeline runs completely serverless on Google Cloud Platform (GCP) within the GCP Free Tier, feeding a BigQuery data warehouse for downstream line-movement tracking, closing line value (CLV) research, and machine learning models.

> 🚀 **New Contributor or Data Scientist?** See the [Quickstart Guide](QUICKSTART.md) for step-by-step instructions on exploring the data schemas and running the CLI.

---

## Architecture Overview

The application follows an event-driven, serverless architecture orchestrated by GCP:

```
                  ┌─────────────────────────────────────┐
                  │      Cloud Scheduler (4-Hourly)     │
                  │   cron: 0 */4 * * * (America/NY)    │
                  └──────────────────┬──────────────────┘
                                     │ OIDC HTTP POST
                                     ▼
                  ┌─────────────────────────────────────┐
                  │    Cloud Run (sports-odds-collector)│
                  │    - Flask / Starlette Container    │
                  │    - Scales to 0 when idle          │
                  │    - Secret Manager (API Keys)      │
                  └─────────┬─────────────────┬─────────┘
                            │                 │
             Fetch Live Odds│                 │Fetch Scores & Metadata
                            ▼                 ▼
             ┌──────────────────────┐  ┌──────────────────────┐
             │    The Odds API      │  │ ESPN Scoreboard API  │
             │   (FanDuel Lines)    │  │ (NFL & NCAAF Scores) │
             └──────────────┬───────┘  └──────────────┬───────┘
                            │                         │
                            │ Streaming Loads         │ Batch Loads
                            ▼                         ▼
                  ┌─────────────────────────────────────┐
                  │          Google BigQuery            │
                  │   Dataset: odds-tracker-402301.     │
                  │            nfl_data                 │
                  │                                     │
                  │  ├── daily_odds_data                │
                  │  ├── game_metadata                  │
                  │  └── game_results                   │
                  └──────────────────┬──────────────────┘
                                     │
                                     ▼
                  ┌─────────────────────────────────────┐
                  │        Streamlit Dashboard          │
                  │    Line Movement & Outcomes UI      │
                  └─────────────────────────────────────┘
```

### Upstream APIs
1. **The Odds API** (`https://api.the-odds-api.com`):
   - **Data Provided**: Real-time betting lines from major US sportsbooks (configured for **FanDuel**).
   - **Markets Captured**: Moneylines (`h2h`), Point Spreads (`spreads`), and Over/Under Totals (`totals`).
   - **Sports Covered**: `americanfootball_nfl` (NFL) and `americanfootball_ncaaf` (College Football).
   - **Quota & Constraints**: Free-tier plan provides **500 requests/month**. The 4-hour scheduled cadence consumes 360 requests/month (2 requests per run × 6 runs/day × 30 days), staying comfortably within quota. Requires `THE_ODDS_API_KEY`.
2. **ESPN Scoreboard API** (`https://site.api.espn.com`):
   - **Data Provided**: Provider-native event metadata, scheduling, venue, status, and final/in-progress team scores.
   - **Endpoints**:
     - NFL: `/apis/site/v2/sports/football/nfl/scoreboard`
     - NCAAF: `/apis/site/v2/sports/football/college-football/scoreboard`
   - **Constraints**: Public endpoint, no API key required.

### NCAA Football Power 4 Filtering
To maximize signal and conserve BigQuery storage and API compute, NCAAF odds collection automatically filters for **Power 4 conferences** (SEC, Big Ten, Big 12, ACC) and high-profile independents (Notre Dame). Matchups between two non-Power 4 teams are filtered out during ingestion.

### GCP Free Tier Footprint
All deployed services operate strictly within Google Cloud's Always Free tier:
- **Cloud Run**: Configured with 1 vCPU and 512 MiB memory; `minInstanceCount = 0` (scales down to 0 when idle). Consumes ~180 invocations and ~4,320 vCPU-seconds per month (<1.2% of the 360,000 vCPU-seconds free tier limit).
- **Cloud Scheduler**: 1 job configured (`odds-collector-four-hourly`) out of 3 free jobs allowed per account.
- **BigQuery**: Dataset `nfl_data` uses ~1.2 MB of storage (<0.02% of the 10 GB free monthly storage limit). Query scans and streaming loads remain well under the 1 TB/month free limit.
- **Cloud Storage**: Bucket `gs://odds-data` stores small CSV snapshots and backup extracts (<0.001% of the 5 GB free storage limit).

---

## Repository File Structure

```text
sports-betting-odds-tracker/
├── app/                              # Core application package
│   ├── config/                       # Configuration module
│   │   ├── conferences.py            # Power 4 team & conference definitions
│   │   └── config.py                 # Dataclasses & YAML/env config loader
│   ├── ddl/                          # Canonical BigQuery schemas
│   │   └── table_schemas.py          # BigQuery SchemaField definitions
│   ├── tests/                        # Comprehensive test suite (pytest)
│   │   ├── test_config.py            # Config loading and env override tests
│   │   ├── test_games_endpoint.py    # ESPN normalization tests
│   │   ├── test_main_server.py       # Cloud Run HTTP handler tests
│   │   ├── test_odds_endpoint.py     # The Odds API query tests
│   │   ├── test_p4_filtering.py      # NCAAF Power 4 filtering tests
│   │   └── test_the_odds_api_parsing.py # Schema & normalization tests
│   ├── gcs_to_bigquery.py            # Pipeline utility to move GCS blobs to BigQuery
│   ├── helper_funcs.py               # Shared transformation and formatting helpers
│   ├── query_betting_api.py          # OddsQuery client for The Odds API
│   └── query_games_api.py            # GamesQuery client for ESPN Scoreboards
├── config/
│   └── config.yaml                   # Base YAML configuration
├── scripts/                          # Operational & exploratory scripts
│   ├── explore_api_data.py           # Offline fixture-based data explorer
│   ├── explore_bigquery_tables.ipynb # BigQuery exploratory analysis notebook
│   ├── explore_odds_and_gcp.ipynb    # Free tier calculator & line movement notebook
│   ├── preview_bigquery_tables.py    # Dry-run DDL generator and schema checker
│   └── reset_bigquery_tables.py      # Guarded table provisioning / reset utility
├── streamlit/                        # Web dashboard interface
│   ├── sql_scripts/                  # Reusable BigQuery analysis queries
│   └── streamlit_app.py              # Interactive odds & line movement app
├── cli.py                            # Developer & data engineering CLI
├── main.py                           # Cloud Run HTTP entrypoint (Flask / Starlette)
├── Dockerfile                        # Production container definition for Cloud Run
├── pyproject.toml                    # Ruff and tool configuration
├── requirements.in                   # Top-level dependencies
├── requirements.txt                  # Pinned production dependencies
├── QUICKSTART.md                     # Quickstart guide for DS & engineers
└── README.md                         # Project documentation
```

---

## Setup & Local Development

### 1. Prerequisites
- Python 3.11 or 3.12
- [Google Cloud SDK (gcloud CLI)](https://cloud.google.com/sdk/docs/install)
- [The Odds API Key](https://the-odds-api.com/) (Free tier)

### 2. Environment Installation

```bash
# Clone the repository
git clone https://github.com/Matt-J-Christy/sports-betting-odds-tracker.git
cd sports-betting-odds-tracker

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.in
```

### 3. Configure Credentials

Create a `.env` file in the project root:

```ini
THE_ODDS_API_KEY=your_odds_api_key_here
GCP_PROJECT_ID=odds-tracker-402301
BIGQUERY_DATASET_ID=nfl_data
GCP_STORAGE_BUCKET=odds-data
```

Authenticate your local environment with Google Cloud:

```bash
gcloud auth application-default login
gcloud config set project odds-tracker-402301
```

### 4. Running the Automated Test Suite

Run the full pytest suite:

```bash
pytest
```

Run code formatting and linting:

```bash
ruff check .
```

---

## Common Workflows

### Manual Data Collection via CLI
Generate or refresh odds, metadata, or scores on demand:

```bash
# Fetch live betting odds in test mode (1 game, saves API quota)
python cli.py get-new-odds --sport all --test-mode

# Query ESPN and ingest current game metadata into BigQuery
python cli.py get-game-metadata

# Query ESPN and ingest game scores/outcomes into BigQuery
python cli.py get-game-scores
```

### Running the Streamlit App
Launch the interactive visualization dashboard locally:

```bash
streamlit run streamlit/streamlit_app.py
```

---

## Contributing

1. Create a feature branch (`git checkout -b feature/my-feature`).
2. Verify all tests pass (`pytest`).
3. Ensure no linting errors are present (`ruff check .`).
4. Commit your changes and open a pull request.

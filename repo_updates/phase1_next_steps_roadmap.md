# Project Index & Phase 1 Next Steps Roadmap

**Project**: Sports Betting Odds Tracker (FanDuel NFL & NCAA Football)  
**Date**: September 2026  
**Status**: In Migration (Transitioning from legacy `api-sports.io` to modern `The Odds API` and GCP Serverless pipeline)  
**Location**: `repo_updates/phase1_next_steps_roadmap.md`

---

## 1. Project Overview & Vision

### Long-Term Goal
Build an end-to-end sports betting intelligence platform for **NFL** and **NCAA Football**:
1. **Automated Line Tracking**: Ingest historical and continuous market lines (Point Spreads, Over/Under Totals, Moneyline) from FanDuel.
2. **Line Movement & Sharp Action Detection**: Detect reverse line movement (RLM), steam moves, and consensus shifts.
3. **Data Science & ML Modeling**: Predict closing line value (CLV), model cover probabilities, and uncover positive expected value (+EV) opportunities.
4. **Interactive UI**: Deliver a responsive Streamlit dashboard for real-time matchup analysis and historical trend visualizations.

### Current Phase 1 Goal
Complete the data collection process and deploy an automated ingestion pipeline that collects odds on an **hourly basis (or optimized cadence)** while **strictly remaining within the Free Tier of all services** (specifically the 500 requests/month quota of The Odds API and GCP free tier thresholds).

---

## 2. Comprehensive Codebase Index & Architecture Audit

The repository contains a mixture of **modernized code**, **legacy components from the initial `api-sports.io` prototype**, and **scaffolding**.

```
sports-betting-odds-tracker/
├── .env                                # Local secrets (gitignored; contains THE_ODDS_API_KEY)
├── .gitignore                          # Ignores venv, .env, __pycache__, .pytest_cache
├── app/
│   ├── config/
│   │   └── config.py                   # [MODERN] Loads .env, THE_ODDS_API_KEY, GCP project & ADC
│   ├── ddl/
│   │   ├── table_schemas.py            # [MODERN] BigQuery schemas (odds_schema, metadata, scores)
│   │   └── daily_odds_data_ddl.py      # [NEEDS FIX] Standalone DDL query script (has SQL typo on line 20)
│   ├── query_betting_api.py            # [IN PROGRESS] Fetches NFL FanDuel lines from The Odds API
│   ├── query_games_api.py              # [LEGACY] Queries old api-sports.io (stale endpoints & headers)
│   ├── gcs_to_bigquery.py              # [NEEDS REFACTOR] Loads GCS to BigQuery; deduplication logic blocks time-series!
│   ├── metadata_gcs_to_bq.py           # [LEGACY] Old api-sports metadata loader
│   ├── scores_gcs_to_bq.py             # [LEGACY] Old api-sports scores loader
│   ├── helper_funcs.py                 # [LEGACY] Old api-sports API request wrappers
│   └── tests/
│       ├── test_the_odds_api_parsing.py# [MODERN] Unit test verifying FanDuel outcome extraction
│       ├── test_exploration_scripts.py # [MODERN] Unit test verifying fixture loaders and schemas
│       ├── test_odds_endpoint.py       # [LEGACY/FAILING] Makes live network call to api-sports.io on import
│       └── test_games_endpoint.py      # [LEGACY/FAILING] Makes live network call to api-sports.io on import
├── cli.py                              # [LEGACY/IN PROGRESS] Click CLI combining old & new commands
├── repo_updates/
│   ├── fanduel_tracker_reference.md    # [BLUEPRINT] Target Cloud Run + BigQuery + Streamlit architecture
│   ├── gcp_security_guide.md           # [GUIDE] Secret Manager & IAM permissions guide
│   └── progress_notes_2026_08_31.md    # [NOTES] Prior session notes
├── requirements.txt                    # [MODERN] Lightweight runtime dependencies (Flask, BQ, requests)
├── requirements.in                     # Full dependencies list
├── scripts/
│   ├── explore_api_data.py             # [CLI TOOL] Terminal script to preview fixtures or live API
│   ├── preview_bigquery_tables.py      # [CLI TOOL] Terminal script for table creation & inspection
│   └── explore_odds_and_gcp.ipynb      # [NEW JUPYTER NOTEBOOK] Interactive API and GCP exploration
└── streamlit/
    └── streamlit_app.py                # [SKELETON] Streamlit UI mockup with hardcoded sample rows
```

### Critical Issues Identified in Current Code
1. **Deduplication Prevents Time-Series Movement (`app/gcs_to_bigquery.py`)**:
   - `write_to_bq` joins `odds_data` with existing records on `['game_id', 'market_type', 'team_or_outcome', 'market_point']` and only inserts rows that have never been seen (`left_only`).
   - *Impact*: When a line moves or stays steady across snapshots, snapshots are discarded! For line tracking, we must append snapshots with `snapshot_time`.
2. **NCAA Football Not Yet Queried (`app/query_betting_api.py`)**:
   - Currently hardcoded to `sport_key = "americanfootball_nfl"`. Needs to iterate over `["americanfootball_nfl", "americanfootball_ncaaf"]`.
3. **Single Daily File Overwrite in GCS**:
   - `write_to_gcs` names the blob `f"betting_odds_{write_date}/data.csv"`. Running hourly overwrites the previous hour's file. Needs timestamping `betting_odds_{write_date}/{write_time}.csv` or direct insertion to BigQuery as outlined in `fanduel_tracker_reference.md`.
4. **Syntax Error in `app/ddl/daily_odds_data_ddl.py`**:
   - Line 20: `CREATE OR REPLACE TABLE `{}.{}.{}"` opens with a backtick and ends with a double-quote.
5. **Legacy Tests Break `pytest` Run**:
   - `test_odds_endpoint.py` and `test_games_endpoint.py` execute top-level network requests to `v1.american-football.api-sports.io` during pytest discovery, throwing unhandled connection errors.

---

## 3. Free Tier Engineering & Quota Math

### The Odds API Limits
- **Free Quota**: **500 requests per month**.
- **Request Mechanics**:
  - Requesting `markets=h2h,spreads,totals` across US bookmakers counts as **1 single request**.
  - Querying NFL (`americanfootball_nfl`) = 1 request.
  - Querying NCAA Football (`americanfootball_ncaaf`) = 1 request.
  - Total per complete snapshot = **2 requests**.
- **The 24/7 Problem**:
  - Polling every hour 24/7 = 24 hours × 30 days = 720 runs = **1,440 requests/month** (300% over the free limit!).

### Schedule Optimization Strategies

| Strategy | Cadence | Total Monthly Requests | Feasible on Free Tier? |
| :--- | :--- | :--- | :--- |
| **A. 24/7 Hourly** | Every hour, 24/7 | 1,440 | ❌ No (exceeds 500) |
| **B. 24/7 Every 3 Hours** | Every 3 hours (8 runs/day) | 480 | ✅ Yes (< 500) |
| **C. Game-Window Hourly (Recommended)** | Hourly during active football windows | ~460 | ✅ **Yes (Best for Line Tracking!)** |

### Recommended Schedule: Game-Window Polling
Football markets move dynamically during game preparation and game days. Mid-week (Tuesday/Wednesday) lines are mostly static.

```
Thursday: 16:00 - 23:00 (NFL Thursday Night Football + NCAA)  ->  8 runs (16 requests)
Friday:   17:00 - 23:00 (NCAA Friday matchups)                 ->  7 runs (14 requests)
Saturday: 10:00 - 23:00 (College Football Game Day)            -> 14 runs (28 requests)
Sunday:   10:00 - 23:00 (NFL Sunday Game Day)                  -> 14 runs (28 requests)
Monday:   17:00 - 23:00 (Monday Night Football)                ->  7 runs (14 requests)
Tue/Wed:  Twice daily baseline (12:00, 18:00)                  ->  4 runs ( 8 requests)
-----------------------------------------------------------------------------------------
Total per week: ~54 runs × 2 requests = 108 requests/week
Total for 4.2 weeks = ~454 requests / month (< 500 free tier limit!)
```

### Free Data for Scores & Game Metadata
- Don't burn Odds API quota on scores!
- **Free Alternative**: Use ESPN's public endpoints (no API key, unlimited free requests):
  - NFL: `https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard`
  - NCAAF: `https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard`

---

## 4. Phase 1 Execution Plan (Step-by-Step)

```
[Phase 1 Roadmap]
  ├── Step 1: Ingestion Pipeline Refinement
  │     ├── Multi-league support (NFL + NCAAF)
  │     ├── Direct BigQuery insertion (or timestamped GCS)
  │     └── Fix time-series deduplication
  ├── Step 2: Quota-Guarded Scheduling
  │     ├── Cloud Scheduler cron expression
  │     └── Runtime header check (`x-requests-remaining`)
  ├── Step 3: Deployment to GCP (Cost-to-Zero)
  │     ├── Cloud Run service or Cloud Run Job
  │     └── Secret Manager binding for THE_ODDS_API_KEY
  ├── Step 4: Streamlit UI Connection
  │     ├── Connect BigQuery client to `daily_odds_data`
  │     └── Render dynamic Plotly line charts
  └── Step 5: Deprecation & Test Cleanup
        ├── Remove/archive legacy `api-sports.io` scripts
        └── Ensure `pytest` passes 100% cleanly
```

### Step 1: Refactor the Ingestion Logic
- Update `app/query_betting_api.py`:
  - Accept a list of leagues: `["americanfootball_nfl", "americanfootball_ncaaf"]`.
  - Check `res.headers.get("x-requests-remaining")` and log remaining quota.
  - Provide a direct `insert_to_bigquery(df)` method using `bigquery.Client().insert_rows_json` or batch load to eliminate redundant GCS hops if desired, or ensure GCS paths include UTC timestamps: `f"betting_odds/{snapshot_timestamp}.csv"`.
  - Fix `gcs_to_bigquery.py` so it appends chronological snapshots rather than filtering out existing `(game_id, market_type, team_or_outcome)`.

### Step 2: Configure Cloud Scheduler & Cloud Run (Serverless)
- Deploy as a lightweight Cloud Run Job or HTTP container (using `repo_updates/Dockerfile` and `requirements.txt`).
- Use Cloud Scheduler to trigger the service on the Game-Window schedule:
  - `0 10-23 * * 6,0` (Saturday & Sunday every hour from 10am to 11pm)
  - `0 16-23 * * 4,5,1` (Thursday, Friday, Monday every hour from 4/5pm to 11pm)
  - `0 12,18 * * 2,3` (Tuesday & Wednesday baseline checks)
- Cloud Run scales to 0 instances when idle, incurring **$0.00 infrastructure cost**.

### Step 3: Connect BigQuery to the Streamlit Dashboard
- Update `streamlit/streamlit_app.py`:
  - Replace static mock rows with a live query against `daily_odds_data`.
  - Filter games by active commence dates (`commence_time >= CURRENT_TIMESTAMP()`).
  - Render tabs for:
    - **Spread Movement**: `market_type == 'spreads'` (Plotly line chart over `snapshot_time`).
    - **Total Movement**: `market_type == 'totals'` (Plotly line chart over `snapshot_time`).
    - **Moneyline Movement**: `market_type == 'h2h'` (Plotly line chart of American odds price).

### Step 4: Clean Up Technical Debt & Legacy Scripts
- Fix the SQL typo in `app/ddl/daily_odds_data_ddl.py`.
- Archive or update legacy files that depend on `api-sports.io`:
  - `app/query_games_api.py`
  - `app/metadata_gcs_to_bq.py`
  - `app/scores_gcs_to_bq.py`
  - `app/helper_funcs.py`
  - `app/tests/test_odds_endpoint.py`
  - `app/tests/test_games_endpoint.py`

---

## 5. Next Session Immediate Action Checklist
- [ ] Open and run the new Jupyter notebook: `scripts/explore_odds_and_gcp.ipynb`.
- [ ] Confirm live data pull for both NFL and NCAAF FanDuel lines.
- [ ] Check `x-requests-remaining` header to establish current quota baseline.
- [ ] Initialize BigQuery table schema using `scripts/preview_bigquery_tables.py --table odds --create` or the notebook cell.
- [ ] Execute an end-to-end dry-run write to BigQuery.


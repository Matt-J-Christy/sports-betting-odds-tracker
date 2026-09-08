# FanDuel Football Odds Tracker Reference Guide

This reference document compiles the complete GCP infrastructure design, data schemas, ingestion scripts, and dashboard layout required to build your FanDuel sports betting tracker for NFL and NCAA Football.

---

## 🏗️ Architecture Blueprint

```
[Cloud Scheduler] 
       │ (Triggers every 1-2 hours)
       ▼
[Cloud Run Ingestion Script] ───► Fetches FanDuel Lines (The Odds API)
       │
       └───► [BigQuery Data Warehouse] (Appends chronological timeline rows)
```

---

## 🗄️ 1. BigQuery Database Schema

Execute this SQL query in your Google Cloud BigQuery console to initialize the table. It utilizes a `market_point` column to cleanly store spreads (`-7.5`) and totals (`48.5`) while letting moneylines evaluate to `NULL`.

```sql
CREATE OR REPLACE TABLE `your_project.sports_odds.fanduel_history` (
    snapshot_time TIMESTAMP,
    game_id STRING,
    sport STRING,
    away_team STRING,
    home_team STRING,
    commence_time TIMESTAMP,
    market_type STRING,      -- h2h, spreads, or totals
    team_or_outcome STRING,  -- Team Name, or 'Over' / 'Under'
    price INT64,             -- American odds value (+110, -105)
    market_point FLOAT64     -- The point spread value or over/under line (NULL for moneyline)
);
```

---

## 🚀 2. Ingestion Service (Python / Cloud Run)

This service targets **NFL** and **NCAA Football** leagues, requesting moneyline (`h2h`), `spreads`, and `totals` in a unified query string to maximize data recovery while optimizing free tier limits.

### Prerequisites (`requirements.txt`)
```text
google-cloud-bigquery
requests
flask
```

### Script Execution (`main.py`)
```python
import os
from datetime import datetime
import requests
from google.cloud import bigquery
from flask import Flask

app = Flask(__name__)
bq_client = bigquery.Client()
TABLE_ID = "your_project.sports_odds.fanduel_history"

FOOTBALL_LEAGUES = ["americanfootball_nfl", "americanfootball_ncaaf"]
BET_TYPES = "h2h,spreads,totals"

@app.route("/", methods=["POST", "GET"])
def trigger_pipeline():
    api_key = os.getenv("THE_ODDS_API_KEY")
    rows_to_insert = []
    snapshot_time = datetime.utcnow().isoformat()

    for sport in FOOTBALL_LEAGUES:
        url = f"https://api.the-odds-api.com/v4/sports/{sport}/odds/"
        params = {
            "apiKey": api_key,
            "regions": "us",
            "markets": BET_TYPES,
            "oddsFormat": "american"
        }
        
        res = requests.get(url, params=params)
        if res.status_code != 200:
            continue
            
        events = res.json()

        for event in events:
            game_id = event.get("id")
            home_team = event.get("home_team")
            away_team = event.get("away_team")
            commence_time = event.get("commence_time")
            
            for bookmaker in event.get("bookmakers", []):
                if bookmaker.get("key").lower() == "fanduel":
                    for market in bookmaker.get("markets", []):
                        market_type = market.get("key")
                        
                        for outcome in market.get("outcomes", []):
                            rows_to_insert.append({
                                "snapshot_time": snapshot_time,
                                "game_id": game_id,
                                "sport": sport,
                                "away_team": away_team,
                                "home_team": home_team,
                                "commence_time": commence_time,
                                "market_type": market_type,
                                "team_or_outcome": outcome.get("name"),
                                "price": int(outcome.get("price")),
                                "market_point": outcome.get("point")
                            })

    if rows_to_insert:
        errors = bq_client.insert_rows_json(TABLE_ID, rows_to_insert)
        if errors:
            return f"BigQuery Errors: {errors}", 500
        return f"Successfully logged {len(rows_to_insert)} football rows to BigQuery.", 200
        
    return "No FanDuel lines available at this snapshot.", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
```

---

## 📊 3. Web Dashboard UI (Streamlit & Plotly Express)

Because you have 1-5 users, this script interfaces directly with BigQuery. It structures line variance analytics directly inside specialized UI tabs.

### Prerequisites (`requirements.txt`)
```text
streamlit
google-cloud-bigquery
plotly
pandas
```

### Dashboard File (`app.py`)
```python
import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery

bq_client = bigquery.Client()

st.set_page_config(page_title="FanDuel Football Odds Tracker", layout="wide")
st.title("🏈 FanDuel Line Movement Tracker")

@st.cache_data(ttl=600)
def get_active_games():
    query = f"""
        SELECT DISTINCT game_id, sport, away_team, home_team 
        FROM `your_project.sports_odds.fanduel_history`
        WHERE commence_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)
    """
    return bq_client.query(query).to_dataframe()

games_df = get_active_games()

if games_df.empty:
    st.warning("No recent game data found in BigQuery.")
else:
    games_df['dropdown_label'] = games_df['sport'].str.upper() + ": " + games_df['away_team'] + " @ " + games_df['home_team']
    selected_label = st.selectbox("Select a Matchup:", games_df['dropdown_label'])
    target_game_id = games_df[games_df['dropdown_label'] == selected_label]['game_id'].values

    odds_query = f"""
        SELECT snapshot_time, market_type, team_or_outcome, price, market_point
        FROM `your_project.sports_odds.fanduel_history`
        WHERE game_id = @game_id
        ORDER BY snapshot_time ASC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("game_id", "STRING", target_game_id)]
    )
    df_trends = bq_client.query(odds_query, job_config=job_config).to_dataframe()

    tab1, tab2, tab3 = st.tabs(["Spreads Line", "Over/Under Totals", "Moneyline Prices"])

    with tab1:
        st.subheader("Spread Movement")
        spread_df = df_trends[df_trends['market_type'] == 'spreads']
        if not spread_df.empty:
            fig_spread = px.line(spread_df, x='snapshot_time', y='market_point', color='team_or_outcome',
                                 title="Spread Variance Over Time", labels={'market_point': 'Spread Points'})
            st.plotly_chart(fig_spread, use_container_width=True)

    with tab2:
        st.subheader("Total (Over/Under) Movement")
        totals_df = df_trends[df_trends['market_type'] == 'totals']
        if not totals_df.empty:
            fig_total = px.line(totals_df, x='snapshot_time', y='market_point', color='team_or_outcome',
                                title="O/U Line Variance Over Time", labels={'market_point': 'Total Points'})
            st.plotly_chart(fig_total, use_container_width=True)

    with tab3:
        st.subheader("Moneyline Price Fluctuations")
        h2h_df = df_trends[df_trends['market_type'] == 'h2h']
        if not h2h_df.empty:
            fig_ml = px.line(h2h_df, x='snapshot_time', y='price', color='team_or_outcome',
                             title="American Odds Movement (+/-)", labels={'price': 'American Odds'})
            st.plotly_chart(fig_ml, use_container_width=True)
```

---

## 🛠️ 4. Deployment Instructions

### Local Testing
Verify your command line terminal has appropriate credentials bound to your active GCP project space:
```bash
gcloud auth application-default login
streamlit run app.py
```

### Scaling Cost-To-Zero Production Rules
1. Build code resources using `gcloud run deploy` routines directly.
2. Map a pipeline execution cadence inside Cloud Scheduler using `0 */2 * * *` (Interval check every 2 hours).
3. Cloud Run properties automatically baseline down to `0 instances` when idle, keeping usage footprint well inside the free tier boundaries.
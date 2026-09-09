"""FanDuel NFL & NCAA Football Odds Movement Tracker Dashboard."""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px

import streamlit as st

# Setup project root import
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config.config import DATASET_ID, GCP_PROJECT_ID, gcp_service_accnt  # noqa: E402

st.set_page_config(
    page_title="FanDuel Football Odds Tracker",
    page_icon="🏈",
    layout="wide",
)

st.title("🏈 FanDuel Line Movement Tracker")
st.caption("Track Point Spreads, Over/Under Totals, and Moneyline odds fluctuations across NFL & NCAA Football.")

TARGET_TABLE = f"{GCP_PROJECT_ID}.{DATASET_ID}.daily_odds_data"


def get_demo_data() -> pd.DataFrame:
    """Generate realistic multi-snapshot demo data for testing when BigQuery is offline."""
    snapshots = [
        "2026-09-04T12:00:00Z",
        "2026-09-05T12:00:00Z",
        "2026-09-06T12:00:00Z",
        "2026-09-07T12:00:00Z",
    ]
    rows = [
        # Game 1: NFL Bills @ Chiefs
        {"snapshot_time": snapshots[0], "game_id": "demo_nfl_1", "sport": "americanfootball_nfl", "away_team": "Buffalo Bills", "home_team": "Kansas City Chiefs", "commence_time": "2026-09-10T20:20:00Z", "market_type": "spreads", "team_or_outcome": "Buffalo Bills", "price": -110, "market_point": -2.5},
        {"snapshot_time": snapshots[1], "game_id": "demo_nfl_1", "sport": "americanfootball_nfl", "away_team": "Buffalo Bills", "home_team": "Kansas City Chiefs", "commence_time": "2026-09-10T20:20:00Z", "market_type": "spreads", "team_or_outcome": "Buffalo Bills", "price": -115, "market_point": -2.5},
        {"snapshot_time": snapshots[2], "game_id": "demo_nfl_1", "sport": "americanfootball_nfl", "away_team": "Buffalo Bills", "home_team": "Kansas City Chiefs", "commence_time": "2026-09-10T20:20:00Z", "market_type": "spreads", "team_or_outcome": "Buffalo Bills", "price": -110, "market_point": -3.0},
        {"snapshot_time": snapshots[3], "game_id": "demo_nfl_1", "sport": "americanfootball_nfl", "away_team": "Buffalo Bills", "home_team": "Kansas City Chiefs", "commence_time": "2026-09-10T20:20:00Z", "market_type": "spreads", "team_or_outcome": "Buffalo Bills", "price": -105, "market_point": -3.5},
        {"snapshot_time": snapshots[0], "game_id": "demo_nfl_1", "sport": "americanfootball_nfl", "away_team": "Buffalo Bills", "home_team": "Kansas City Chiefs", "commence_time": "2026-09-10T20:20:00Z", "market_type": "spreads", "team_or_outcome": "Kansas City Chiefs", "price": -110, "market_point": 2.5},
        {"snapshot_time": snapshots[1], "game_id": "demo_nfl_1", "sport": "americanfootball_nfl", "away_team": "Buffalo Bills", "home_team": "Kansas City Chiefs", "commence_time": "2026-09-10T20:20:00Z", "market_type": "spreads", "team_or_outcome": "Kansas City Chiefs", "price": -105, "market_point": 2.5},
        {"snapshot_time": snapshots[2], "game_id": "demo_nfl_1", "sport": "americanfootball_nfl", "away_team": "Buffalo Bills", "home_team": "Kansas City Chiefs", "commence_time": "2026-09-10T20:20:00Z", "market_type": "spreads", "team_or_outcome": "Kansas City Chiefs", "price": -110, "market_point": 3.0},
        {"snapshot_time": snapshots[3], "game_id": "demo_nfl_1", "sport": "americanfootball_nfl", "away_team": "Buffalo Bills", "home_team": "Kansas City Chiefs", "commence_time": "2026-09-10T20:20:00Z", "market_type": "spreads", "team_or_outcome": "Kansas City Chiefs", "price": -115, "market_point": 3.5},
        {"snapshot_time": snapshots[0], "game_id": "demo_nfl_1", "sport": "americanfootball_nfl", "away_team": "Buffalo Bills", "home_team": "Kansas City Chiefs", "commence_time": "2026-09-10T20:20:00Z", "market_type": "totals", "team_or_outcome": "Over", "price": -110, "market_point": 48.5},
        {"snapshot_time": snapshots[1], "game_id": "demo_nfl_1", "sport": "americanfootball_nfl", "away_team": "Buffalo Bills", "home_team": "Kansas City Chiefs", "commence_time": "2026-09-10T20:20:00Z", "market_type": "totals", "team_or_outcome": "Over", "price": -115, "market_point": 48.5},
        {"snapshot_time": snapshots[2], "game_id": "demo_nfl_1", "sport": "americanfootball_nfl", "away_team": "Buffalo Bills", "home_team": "Kansas City Chiefs", "commence_time": "2026-09-10T20:20:00Z", "market_type": "totals", "team_or_outcome": "Over", "price": -110, "market_point": 47.5},
        {"snapshot_time": snapshots[3], "game_id": "demo_nfl_1", "sport": "americanfootball_nfl", "away_team": "Buffalo Bills", "home_team": "Kansas City Chiefs", "commence_time": "2026-09-10T20:20:00Z", "market_type": "totals", "team_or_outcome": "Over", "price": -108, "market_point": 46.5},
        {"snapshot_time": snapshots[0], "game_id": "demo_nfl_1", "sport": "americanfootball_nfl", "away_team": "Buffalo Bills", "home_team": "Kansas City Chiefs", "commence_time": "2026-09-10T20:20:00Z", "market_type": "h2h", "team_or_outcome": "Buffalo Bills", "price": -135, "market_point": None},
        {"snapshot_time": snapshots[1], "game_id": "demo_nfl_1", "sport": "americanfootball_nfl", "away_team": "Buffalo Bills", "home_team": "Kansas City Chiefs", "commence_time": "2026-09-10T20:20:00Z", "market_type": "h2h", "team_or_outcome": "Buffalo Bills", "price": -145, "market_point": None},
        {"snapshot_time": snapshots[2], "game_id": "demo_nfl_1", "sport": "americanfootball_nfl", "away_team": "Buffalo Bills", "home_team": "Kansas City Chiefs", "commence_time": "2026-09-10T20:20:00Z", "market_type": "h2h", "team_or_outcome": "Buffalo Bills", "price": -160, "market_point": None},
        {"snapshot_time": snapshots[3], "game_id": "demo_nfl_1", "sport": "americanfootball_nfl", "away_team": "Buffalo Bills", "home_team": "Kansas City Chiefs", "commence_time": "2026-09-10T20:20:00Z", "market_type": "h2h", "team_or_outcome": "Buffalo Bills", "price": -175, "market_point": None},
        {"snapshot_time": snapshots[0], "game_id": "demo_nfl_1", "sport": "americanfootball_nfl", "away_team": "Buffalo Bills", "home_team": "Kansas City Chiefs", "commence_time": "2026-09-10T20:20:00Z", "market_type": "h2h", "team_or_outcome": "Kansas City Chiefs", "price": 115, "market_point": None},
        {"snapshot_time": snapshots[1], "game_id": "demo_nfl_1", "sport": "americanfootball_nfl", "away_team": "Buffalo Bills", "home_team": "Kansas City Chiefs", "commence_time": "2026-09-10T20:20:00Z", "market_type": "h2h", "team_or_outcome": "Kansas City Chiefs", "price": 125, "market_point": None},
        {"snapshot_time": snapshots[2], "game_id": "demo_nfl_1", "sport": "americanfootball_nfl", "away_team": "Buffalo Bills", "home_team": "Kansas City Chiefs", "commence_time": "2026-09-10T20:20:00Z", "market_type": "h2h", "team_or_outcome": "Kansas City Chiefs", "price": 135, "market_point": None},
        {"snapshot_time": snapshots[3], "game_id": "demo_nfl_1", "sport": "americanfootball_nfl", "away_team": "Buffalo Bills", "home_team": "Kansas City Chiefs", "commence_time": "2026-09-10T20:20:00Z", "market_type": "h2h", "team_or_outcome": "Kansas City Chiefs", "price": 145, "market_point": None},
        # Game 2: NCAAF Texas @ Michigan
        {"snapshot_time": snapshots[0], "game_id": "demo_ncaaf_1", "sport": "americanfootball_ncaaf", "away_team": "Texas Longhorns", "home_team": "Michigan Wolverines", "commence_time": "2026-09-12T16:00:00Z", "market_type": "spreads", "team_or_outcome": "Texas Longhorns", "price": -110, "market_point": -7.0},
        {"snapshot_time": snapshots[1], "game_id": "demo_ncaaf_1", "sport": "americanfootball_ncaaf", "away_team": "Texas Longhorns", "home_team": "Michigan Wolverines", "commence_time": "2026-09-12T16:00:00Z", "market_type": "spreads", "team_or_outcome": "Texas Longhorns", "price": -110, "market_point": -7.5},
        {"snapshot_time": snapshots[2], "game_id": "demo_ncaaf_1", "sport": "americanfootball_ncaaf", "away_team": "Texas Longhorns", "home_team": "Michigan Wolverines", "commence_time": "2026-09-12T16:00:00Z", "market_type": "spreads", "team_or_outcome": "Texas Longhorns", "price": -105, "market_point": -7.5},
        {"snapshot_time": snapshots[3], "game_id": "demo_ncaaf_1", "sport": "americanfootball_ncaaf", "away_team": "Texas Longhorns", "home_team": "Michigan Wolverines", "commence_time": "2026-09-12T16:00:00Z", "market_type": "spreads", "team_or_outcome": "Texas Longhorns", "price": -110, "market_point": -8.0},
        {"snapshot_time": snapshots[0], "game_id": "demo_ncaaf_1", "sport": "americanfootball_ncaaf", "away_team": "Texas Longhorns", "home_team": "Michigan Wolverines", "commence_time": "2026-09-12T16:00:00Z", "market_type": "totals", "team_or_outcome": "Over", "price": -110, "market_point": 42.5},
        {"snapshot_time": snapshots[1], "game_id": "demo_ncaaf_1", "sport": "americanfootball_ncaaf", "away_team": "Texas Longhorns", "home_team": "Michigan Wolverines", "commence_time": "2026-09-12T16:00:00Z", "market_type": "totals", "team_or_outcome": "Over", "price": -110, "market_point": 43.0},
        {"snapshot_time": snapshots[2], "game_id": "demo_ncaaf_1", "sport": "americanfootball_ncaaf", "away_team": "Texas Longhorns", "home_team": "Michigan Wolverines", "commence_time": "2026-09-12T16:00:00Z", "market_type": "totals", "team_or_outcome": "Over", "price": -108, "market_point": 43.5},
        {"snapshot_time": snapshots[3], "game_id": "demo_ncaaf_1", "sport": "americanfootball_ncaaf", "away_team": "Texas Longhorns", "home_team": "Michigan Wolverines", "commence_time": "2026-09-12T16:00:00Z", "market_type": "totals", "team_or_outcome": "Over", "price": -110, "market_point": 44.0},
    ]
    return pd.DataFrame(rows)


@st.cache_data(ttl=300)
def load_active_games_from_bq() -> tuple[pd.DataFrame | None, str | None]:
    """Query recent and upcoming games from BigQuery."""
    try:
        from google.cloud import bigquery

        client = bigquery.Client(project=GCP_PROJECT_ID, credentials=gcp_service_accnt)
        query = f"""
            SELECT DISTINCT game_id, sport, away_team, home_team, commence_time
            FROM `{TARGET_TABLE}`
            WHERE commence_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)
            ORDER BY commence_time ASC
        """
        df = client.query(query).to_dataframe()
        return df, None
    except Exception as exc:
        return None, str(exc)


@st.cache_data(ttl=180)
def load_game_trend_from_bq(game_id: str) -> pd.DataFrame:
    """Load historical line movements for a specific game_id from BigQuery."""
    from google.cloud import bigquery

    client = bigquery.Client(project=GCP_PROJECT_ID, credentials=gcp_service_accnt)
    query = f"""
        SELECT snapshot_time, game_id, sport, away_team, home_team,
               commence_time, market_type, team_or_outcome, price, market_point
        FROM `{TARGET_TABLE}`
        WHERE game_id = @game_id
        ORDER BY snapshot_time ASC
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("game_id", "STRING", game_id)]
    )
    return client.query(query, job_config=job_config).to_dataframe()


# Sidebar Configuration
with st.sidebar:
    st.header("⚙️ Data Source")
    st.text(f"Project: {GCP_PROJECT_ID}")
    st.text(f"Dataset: {DATASET_ID}")
    st.text("Table:   daily_odds_data")

    if st.button("🔄 Refresh Data Cache"):
        st.cache_data.clear()
        st.rerun()

    st.markdown("---")
    st.markdown("**Free Tier Status**")
    st.markdown("API Budget: **500 requests/month**")
    st.markdown("Ingestion Schedule: **Game Windows (Thu-Mon)**")


# Attempt to fetch live data from BigQuery
games_df, bq_error = load_active_games_from_bq()
is_live = games_df is not None and not games_df.empty

if not is_live:
    st.info("ℹ️ Displaying demonstration mode (BigQuery table not yet populated or offline).")
    all_demo_data = get_demo_data()
    games_df = all_demo_data[["game_id", "sport", "away_team", "home_team", "commence_time"]].drop_duplicates()

# Filters: Sport and Matchup
col_filter1, col_filter2 = st.columns([1, 3])

with col_filter1:
    sport_choice = st.selectbox(
        "Filter League",
        options=["All", "NFL", "NCAA Football"],
    )

if sport_choice == "NFL":
    filtered_games = games_df[games_df["sport"] == "americanfootball_nfl"]
elif sport_choice == "NCAA Football":
    filtered_games = games_df[games_df["sport"] == "americanfootball_ncaaf"]
else:
    filtered_games = games_df

if filtered_games.empty:
    st.warning("No games found for the selected filter.")
    st.stop()

filtered_games = filtered_games.copy()
filtered_games["label"] = (
    filtered_games["sport"].str.replace("americanfootball_", "").str.upper()
    + ": "
    + filtered_games["away_team"]
    + " @ "
    + filtered_games["home_team"]
)

with col_filter2:
    selected_label = st.selectbox(
        "Select Matchup",
        options=filtered_games["label"].tolist(),
    )

selected_game_id = filtered_games.loc[filtered_games["label"] == selected_label, "game_id"].iloc[0]

# Load trend data
if is_live:
    trend_df = load_game_trend_from_bq(selected_game_id)
else:
    trend_df = all_demo_data[all_demo_data["game_id"] == selected_game_id]

if trend_df.empty:
    st.warning("No line movement snapshots available for this matchup.")
    st.stop()

# Matchup Summary Banner
away_team = trend_df["away_team"].iloc[0]
home_team = trend_df["home_team"].iloc[0]
commence_time = trend_df["commence_time"].iloc[0]

col_m1, col_m2, col_m3, col_m4 = st.columns(4)
col_m1.metric("Away Team", away_team)
col_m2.metric("Home Team", home_team)
col_m3.metric("Snapshots Recorded", len(trend_df["snapshot_time"].unique()))
col_m4.metric("Kickoff (UTC)", str(commence_time)[:16].replace("T", " "))

st.markdown("---")

# Visualizations Tabs
tab_spread, tab_totals, tab_moneyline, tab_raw = st.tabs(
    ["📊 Point Spread Movement", "🎯 Over / Under Totals", "💰 Moneyline Prices", "📋 Raw Snapshot Table"]
)

with tab_spread:
    spreads_df = trend_df[trend_df["market_type"] == "spreads"]
    if not spreads_df.empty:
        fig_spread = px.line(
            spreads_df,
            x="snapshot_time",
            y="market_point",
            color="team_or_outcome",
            markers=True,
            title=f"FanDuel Spread Variance: {away_team} @ {home_team}",
            labels={"market_point": "Point Spread", "snapshot_time": "Snapshot Time (UTC)"},
        )
        fig_spread.update_layout(hovermode="x unified", legend_title="Team")
        st.plotly_chart(fig_spread, use_container_width=True)
    else:
        st.info("No spread data recorded for this matchup.")

with tab_totals:
    totals_df = trend_df[trend_df["market_type"] == "totals"]
    if not totals_df.empty:
        fig_totals = px.line(
            totals_df,
            x="snapshot_time",
            y="market_point",
            color="team_or_outcome",
            markers=True,
            title=f"FanDuel Over/Under Line Variance: {away_team} @ {home_team}",
            labels={"market_point": "Total Points Line", "snapshot_time": "Snapshot Time (UTC)"},
        )
        fig_totals.update_layout(hovermode="x unified", legend_title="Outcome")
        st.plotly_chart(fig_totals, use_container_width=True)
    else:
        st.info("No totals data recorded for this matchup.")

with tab_moneyline:
    ml_df = trend_df[trend_df["market_type"] == "h2h"]
    if not ml_df.empty:
        fig_ml = px.line(
            ml_df,
            x="snapshot_time",
            y="price",
            color="team_or_outcome",
            markers=True,
            title=f"FanDuel Moneyline Odds Fluctuations: {away_team} @ {home_team}",
            labels={"price": "American Odds (+/-)", "snapshot_time": "Snapshot Time (UTC)"},
        )
        fig_ml.update_layout(hovermode="x unified", legend_title="Team")
        st.plotly_chart(fig_ml, use_container_width=True)
    else:
        st.info("No moneyline data recorded for this matchup.")

with tab_raw:
    st.subheader("Chronological Snapshot History")
    st.dataframe(trend_df, use_container_width=True)
    csv_data = trend_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="📥 Download Matchup CSV",
        data=csv_data,
        file_name=f"{selected_game_id}_odds_history.csv",
        mime="text/csv",
    )

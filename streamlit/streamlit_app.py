"""
Run streamlit app to visualize games and odds
"""


from google.cloud import bigquery
import sys
import os
import pandas as pd
import streamlit
import plotly.express as px
import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from app.config.config import gcp_service_accnt  # noqa: E402

bq = bigquery.Client(credentials=gcp_service_accnt)

project = 'odds-tracker-402301'
dataset_id = 'nfl_data'
odds_table = 'daily_odds_data'
metadata_table = 'game_metadata'
score_table = 'game_results'


@streamlit.cache_data(ttl=6000)
def run_query(query: str) -> pd.DataFrame:
    return bq.query(query).to_dataframe()


with open('sql_scripts/bets_and_games.sql') as f:
    bets_query = f.read()

with open('sql_scripts/game_results.sql') as f:
    score_query = f.read()


# run queries and cache results

bets = run_query(bets_query)
bets['matchup'] = bets['home_team'] + ' vs. ' + bets['away_team']
# bets[['update_date', 'game_date']] = bets[['update_date', 'game_date']]\
# .apply(pd.to_datetime, axis=1)
bets['is_upcoming'] = bets['game_date'] >= datetime.datetime.now().date()


game_results = run_query(score_query)
game_results['first_half_score_home'] = game_results['home_score_q1'] + \
    game_results['home_score_q2']
game_results['first_half_score_away'] = game_results['away_score_q1'] + \
    game_results['away_score_q2']


# define filters for game date, bet type, match up,
# home or away team and if the game is upcoming or completed

streamlit.title('NFL Betting Odds Tracker')

bet_name_filter = streamlit.multiselect('Select bet type',
                                        options=list(
                                            bets['bet_name'].unique()),
                                        default=list(bets['bet_name'].unique())
                                        )

matchup_filter = streamlit.multiselect('Select matchup',
                                       options=list(bets['matchup'].unique()),
                                       default=list(bets['matchup'].unique())
                                       )

game_date_filter = streamlit.multiselect('Select game date',
                                         options=list(
                                             bets['game_date'].unique()),
                                         default=list(
                                             bets['game_date'].unique())
                                         )

upcoming_filter = streamlit.multiselect('Select upcoming games',
                                        options=[True, False],
                                        default=[True, False]
                                        )

filtered_data = bets.loc[(bets['bet_name'].isin(bet_name_filter)) &
                         (bets['matchup'].isin(matchup_filter)) &
                         (bets['game_date'].isin(game_date_filter)) &
                         (bets['is_upcoming'].isin(upcoming_filter)), :]


# Define a few charts
fig = px.scatter(
    filtered_data,
    x='update_date',
    y='odd',
    color='bet_subgroup',
    # text='matchup',
    facet_col='bet_name',
    facet_col_wrap=2,
    labels={'odd': 'Decimal Odds',
            'update_date': 'Datetime',
            'bet_subgroup': 'Bet Outcome'
            }
)\
    .update_yaxes(matches=None)\
    .for_each_yaxis(lambda yaxis: yaxis.update(showticklabels=True))\
    .for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))


# define plots in streamlit
tab1, tab2, tab3 = streamlit.tabs(
    ['Betting lines for matchups', 'Line Table', 'Scores from previous games'])

with tab1:
    streamlit.plotly_chart(fig, theme='streamlit')

with tab2:
    cols = ['value', 'odd', 'bet_subgroup', 'subgroup_value', 'bet_name',
            'update_date', 'game_date', 'week', 'home_team', 'away_team']
    streamlit.write(filtered_data.loc[:, cols])

with tab3:
    cols = ['week', 'game_date', 'city', 'home_team',
            'away_team', 'first_half_score_home',
            'first_half_score_away', 'home_score_final', 'away_score_final']
    streamlit.write(game_results.loc[:, cols])

    # streamlit.plotly_chart(fig2, theme='streamlit')

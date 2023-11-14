"""
Backfilling data
"""

import pandas as pd
from app.config.config import my_api_key, gcp_service_accnt
from app.metadata_gcs_to_bq import GameMetadataToBq
from app.scores_gcs_to_bq import GameScoresToBq
from app.query_games_api import GamesQuery
import time

query = GamesQuery(api_key=my_api_key,
                   gcp_service_accnt=gcp_service_accnt
                   )

gcs_to_bq = GameMetadataToBq(
    gcs_bucket='odds-data',
    table_id='odds-tracker-402301.nfl_data.game_metadata',
    gcp_service_accnt=gcp_service_accnt
)

scores_gcs_to_bq = GameScoresToBq(
    gcs_bucket='odds-data',
    table_id='odds-tracker-402301.nfl_data.game_results',
    gcp_service_accnt=gcp_service_accnt
)

sundays = pd.date_range(start='2023-10-01',
                        end='2023-11-07',
                        freq='W-SUN').strftime('%Y-%m-%d').tolist()

mondays = pd.date_range(start='2023-10-01',
                        end='2023-11-07',
                        freq='W-MON').strftime('%Y-%m-%d').tolist()


dates = sundays + mondays

# write data to GCS
for date in dates:
    print('getting data for', date)
    try:
        score_data = query.query_game_scores(date=date)
        time.sleep(5)
        game_metadata = query.query_game_metadata(date=date)
        time.sleep(5)
        query.write_to_gcs(data=score_data, data_topic='scores', date=date)
        query.write_to_gcs(data=game_metadata,
                           data_topic='metadata', date=date)
    except:  # noqa
        print('backfill for {} failed'.format(date))


# write data to BQ
for date in dates:
    data = gcs_to_bq.get_latest_gcs_data(date=date)
    gcs_to_bq.write_to_bq(metadata_df=data)

# erg. I scrapped a bunch of data and named a column incorrectly.
for date in dates[1:]:
    score_df = scores_gcs_to_bq.get_latest_gcs_data(date=date)
    score_df = score_df.rename(columns={'home_score_total': 'home_score_final',
                                        'away_score_total': 'away_score_final'})
    scores_gcs_to_bq.write_to_bq(score_df=score_df)

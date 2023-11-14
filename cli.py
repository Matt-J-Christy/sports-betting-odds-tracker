"""
Create CLICK cli file for odds tracking
"""


import click
from app.query_betting_api import OddsQuery
from app.gcs_to_bigquery import GcsToBq
from app.query_games_api import GamesQuery
from app.metadata_gcs_to_bq import GameMetadataToBq
from app.scores_gcs_to_bq import GameScoresToBq
from app.config.config import my_api_key, gcp_service_accnt
from typing import Optional


@click.group()
def cli():
    pass


@cli.command()
@click.option('--test-mode',
              default=False,
              help='Used to limit number of API calls')
def get_new_odds(test_mode: bool):
    """
    Command to query API for next Sunday's NFL betting odds.
    The OddsQuery class does the work of querying the betting API
    and writing results to GCS
    """
    click.echo('getting next weeks NFL betting odds')

    odds_query = OddsQuery(
        api_key=my_api_key, gcp_service_accnt=gcp_service_accnt)
    odds_data = odds_query.get_new_odds(test_mode=test_mode)
    odds_query.write_to_gcs(betting_data=odds_data)


@cli.command()
def get_game_metadata():
    """
    Command to get metadata from the game.
    Home team, away team, location, start time
    """
    click.echo('Querying game metadata')
    games_query = GamesQuery(
        api_key=my_api_key, gcp_service_accnt=gcp_service_accnt)
    game_metadata = games_query.query_game_metadata()
    games_query.write_to_gcs(data=game_metadata, data_topic='metadata')


@cli.command()
def get_game_scores():
    """
    Command to get score data from the prior
    week's NFL games. Data is stored by quarter
    """
    click.echo('Querying API for game scores')
    games_query = GamesQuery(
        api_key=my_api_key, gcp_service_accnt=gcp_service_accnt)
    score_data = games_query.query_game_scores()
    games_query.write_to_gcs(data=score_data, data_topic='scores')


@cli.command()
@click.option('--date',
              default=None,
              help='Move latest data to BQ or specify specific date for backfill')
def write_odds_to_bq(date: Optional[str] = None):
    """
    Command to move latest GCS data to BigQuery
    """
    click.echo('refreshing odds table with new CGS data')
    gcs_to_bq = GcsToBq(gcp_service_accnt=gcp_service_accnt,
                        gcs_bucket='odds-data',
                        table_id='odds-tracker-402301.nfl_data.daily_odds_data'
                        )
    gcs_data = gcs_to_bq.get_latest_gcs_data(date=date)
    gcs_to_bq.write_to_bq(odds_data=gcs_data)


@cli.command()
@click.option('--date',
              default=None,
              help='Move latest data to BQ or specify specific date for backfill')
def game_metadata_to_bq(date: Optional[str] = None):
    """
    writing game metadata to Bigquery table
    """
    click.echo('writing game metadata from GCS to Bigquery')
    gcs_to_bq = GameMetadataToBq(gcp_service_accnt=gcp_service_accnt,
                                 gcs_bucket='odds-data',
                                 table_id='odds-tracker-402301.nfl_data.game_metadata'
                                 )
    gcs_data = gcs_to_bq.get_latest_gcs_data(date=date)
    gcs_to_bq.write_to_bq(metadata_df=gcs_data)


@cli.command()
@click.option('--date',
              default=None,
              help='Move latest data to BQ or specify specific date for backfill')
def game_scores_to_bq(date: Optional[str] = None):
    """
    Writing completed game scores to bigquery
    """
    click.echo('reading scores data from GCS and writing to Bigquery')
    gcs_to_bq = GameScoresToBq(gcp_service_accnt=gcp_service_accnt,
                               gcs_bucket='oods-data',
                               table_id='odds-tracker-402301.nfl_data.game_results')
    gcs_data = gcs_to_bq.get_latest_gcs_data(date=date)
    gcs_to_bq.write_to_bq(score_df=gcs_data)


cli.add_command(get_new_odds)
cli.add_command(write_odds_to_bq)
cli.add_command(get_game_metadata)
cli.add_command(get_game_scores)
cli.add_command(game_metadata_to_bq)
cli.add_command(game_scores_to_bq)

if __name__ == '__main__':
    cli()

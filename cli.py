"""
Create CLICK cli file for odds tracking
"""


import datetime

import click

from app.config.config import DATASET_ID, GCP_PROJECT_ID, gcp_service_accnt, my_api_key
from app.gcs_to_bigquery import GcsToBq
from app.metadata_gcs_to_bq import GameMetadataToBq
from app.query_betting_api import OddsQuery
from app.query_games_api import GamesQuery
from app.scores_gcs_to_bq import GameScoresToBq


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--sport",
    type=click.Choice(["all", "nfl", "ncaaf"], case_sensitive=False),
    default="all",
    help="Sport to pull odds for (all, nfl, ncaaf)",
)
@click.option(
    "--destination",
    type=click.Choice(["bq", "gcs", "both", "none"], case_sensitive=False),
    default="bq",
    help="Where to persist the fetched odds",
)
@click.option(
    "--test-mode",
    is_flag=True,
    default=False,
    help="Limit API results to a single event for testing",
)
def get_new_odds(sport: str, destination: str, test_mode: bool):
    """Query The Odds API for FanDuel betting odds across NFL and NCAA Football."""
    click.echo(f"Fetching betting odds (sport={sport}, test_mode={test_mode})...")

    if sport == "nfl":
        sports = ["americanfootball_nfl"]
    elif sport == "ncaaf":
        sports = ["americanfootball_ncaaf"]
    else:
        sports = ["americanfootball_nfl", "americanfootball_ncaaf"]

    odds_query = OddsQuery(api_key=my_api_key, gcp_service_accnt=gcp_service_accnt)
    odds_data = odds_query.get_new_odds(sports=sports, test_mode=test_mode)

    click.echo(f"Retrieved {len(odds_data)} snapshot rows.")

    if destination in ("bq", "both"):
        target_table = f"{GCP_PROJECT_ID}.{DATASET_ID}.daily_odds_data"
        click.echo(f"Writing to BigQuery table: {target_table}")
        odds_query.write_to_bq(betting_data=odds_data, table_id=target_table)

    if destination in ("gcs", "both"):
        click.echo("Writing to GCS bucket...")
        odds_query.write_to_gcs(betting_data=odds_data)

    for s_key, q_info in odds_query.last_quota_info.items():
        click.echo(
            f"Quota [{s_key}]: Remaining = {q_info.get('requests_remaining')}, "
            f"Used = {q_info.get('requests_used')}"
        )


@cli.command()
@click.option('--date',
              default=None,
              help='Used to specify specific day for game metadata')
def get_game_metadata(date: datetime.date | None = None):
    """
    Command to get metadata from the game.
    Home team, away team, location, start time
    """
    click.echo('Querying game metadata')
    games_query = GamesQuery(
        api_key=my_api_key, gcp_service_accnt=gcp_service_accnt)
    game_metadata = games_query.query_game_metadata(date=date)
    games_query.write_to_gcs(data=game_metadata, data_topic='metadata')


@cli.command()
@click.option('--date',
              default=None,
              help='Used to specify specific day for game scores')
def get_game_scores(date: datetime.date | None = None):
    """
    Command to get score data for NFL games.
    Data is stored by quarter and users
    can specify a specific date
    """
    click.echo('Querying API for game scores')
    games_query = GamesQuery(
        api_key=my_api_key, gcp_service_accnt=gcp_service_accnt)
    score_data = games_query.query_game_scores(date=date)
    games_query.write_to_gcs(data=score_data, data_topic='scores')


@cli.command()
@click.option('--date',
              default=None,
              help='Move latest data to BQ or specify specific date for backfill')
def write_odds_to_bq(date: str | None = None):
    """
    Command to move latest GCS data to BigQuery
    """
    click.echo('refreshing odds table with new CGS data')
    gcs_to_bq = GcsToBq(gcp_service_accnt=gcp_service_accnt,
                        gcs_bucket='odds-data',
                        table_id=f'{GCP_PROJECT_ID}.{DATASET_ID}.daily_odds_data'
                        )
    gcs_data = gcs_to_bq.get_latest_gcs_data(date=date)
    gcs_to_bq.write_to_bq(odds_data=gcs_data)


@cli.command()
@click.option('--date',
              default=None,
              help='Move latest data to BQ or specify specific date for backfill')
def game_metadata_to_bq(date: str | None = None):
    """
    writing game metadata to Bigquery table
    """
    click.echo('writing game metadata from GCS to Bigquery')
    gcs_to_bq = GameMetadataToBq(gcp_service_accnt=gcp_service_accnt,
                                 gcs_bucket='odds-data',
                                 table_id=f'{GCP_PROJECT_ID}.{DATASET_ID}.game_metadata'
                                 )
    gcs_data = gcs_to_bq.get_latest_gcs_data(date=date)
    gcs_to_bq.write_to_bq(metadata_df=gcs_data)


@cli.command()
@click.option('--date',
              default=None,
              help='Move latest data to BQ or specify specific date for backfill')
def game_scores_to_bq(date: str | None = None):
    """
    Writing completed game scores to bigquery
    """
    click.echo('reading scores data from GCS and writing to Bigquery')
    gcs_to_bq = GameScoresToBq(gcp_service_accnt=gcp_service_accnt,
                               gcs_bucket='odds-data',
                               table_id=f'{GCP_PROJECT_ID}.{DATASET_ID}.game_results')
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

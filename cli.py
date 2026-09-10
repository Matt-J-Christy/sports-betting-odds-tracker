"""
Create CLICK cli file for odds tracking
"""


import datetime

import click
import pandas as pd

from app.config.config import DATASET_ID, GCP_PROJECT_ID, gcp_service_accnt, my_api_key
from app.ddl.table_schemas import game_metadata_schema, game_score_schema
from app.gcs_to_bigquery import GcsToBq
from app.query_betting_api import OddsQuery
from app.query_games_api import GamesQuery


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
@click.option('--date', type=click.DateTime(formats=['%Y-%m-%d']), default=None)
def get_game_metadata(date: datetime.datetime | None = None):
    """Query ESPN event metadata and append it to BigQuery."""
    click.echo('Querying ESPN game metadata')
    games_query = GamesQuery(api_key=my_api_key, gcp_service_accnt=gcp_service_accnt)
    game_metadata = games_query.query_game_metadata(date=date.date() if date else None)
    target_table = f'{GCP_PROJECT_ID}.{DATASET_ID}.game_metadata'
    from google.cloud import bigquery

    game_metadata = game_metadata.copy()
    for field in game_metadata_schema:
        if field.field_type == 'TIMESTAMP' and field.name in game_metadata.columns:
            game_metadata[field.name] = pd.to_datetime(game_metadata[field.name], utc=True)
    client = bigquery.Client(credentials=gcp_service_accnt)
    client.load_table_from_dataframe(
        game_metadata,
        target_table,
        job_config=bigquery.LoadJobConfig(schema=game_metadata_schema),
    ).result()


@cli.command()
@click.option('--date', type=click.DateTime(formats=['%Y-%m-%d']), default=None)
def get_game_scores(date: datetime.datetime | None = None):
    """Query ESPN game scores and append them to BigQuery."""
    click.echo('Querying ESPN game scores')
    games_query = GamesQuery(api_key=my_api_key, gcp_service_accnt=gcp_service_accnt)
    score_data = games_query.query_game_scores(date=date.date() if date else None)
    target_table = f'{GCP_PROJECT_ID}.{DATASET_ID}.game_results'
    from google.cloud import bigquery

    score_data = score_data.copy()
    for field in game_score_schema:
        if field.field_type == 'TIMESTAMP' and field.name in score_data.columns:
            score_data[field.name] = pd.to_datetime(score_data[field.name], utc=True)
    client = bigquery.Client(credentials=gcp_service_accnt)
    client.load_table_from_dataframe(
        score_data,
        target_table,
        job_config=bigquery.LoadJobConfig(schema=game_score_schema),
    ).result()


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


cli.add_command(get_new_odds)
cli.add_command(write_odds_to_bq)
cli.add_command(get_game_metadata)
cli.add_command(get_game_scores)

if __name__ == '__main__':
    cli()

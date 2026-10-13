"""
Create CLICK cli file for odds tracking
"""


import datetime

import click

from app.config.config import DATASET_ID, GCP_PROJECT_ID, gcp_service_accnt, my_api_key
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
    """Query ESPN event metadata and upsert it into BigQuery."""
    click.echo('Querying ESPN game metadata...')
    games_query = GamesQuery(api_key=my_api_key, gcp_service_accnt=gcp_service_accnt)
    game_metadata = games_query.query_game_metadata(date=date.date() if date else None)
    click.echo(f'Fetched {len(game_metadata)} metadata rows. Upserting into BigQuery...')
    affected = games_query.upsert_to_bq(game_metadata, table_type='metadata')
    click.echo(f'Upsert completed successfully ({affected} rows affected).')


@cli.command()
@click.option('--date', type=click.DateTime(formats=['%Y-%m-%d']), default=None)
def get_game_scores(date: datetime.datetime | None = None):
    """Query ESPN game scores and upsert them into BigQuery."""
    click.echo('Querying ESPN game scores...')
    games_query = GamesQuery(api_key=my_api_key, gcp_service_accnt=gcp_service_accnt)
    score_data = games_query.query_game_scores(date=date.date() if date else None)
    click.echo(f'Fetched {len(score_data)} score rows. Upserting into BigQuery...')
    affected = games_query.upsert_to_bq(score_data, table_type='scores')
    click.echo(f'Upsert completed successfully ({affected} rows affected).')



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
def get_ncaa_rankings():
    """Query ESPN NCAA AP Top 25 rankings and upsert into BigQuery."""
    click.echo('Querying ESPN NCAA AP Top 25 rankings...')
    games_query = GamesQuery(api_key=my_api_key, gcp_service_accnt=gcp_service_accnt)
    rankings_df = games_query.query_ncaa_rankings()
    click.echo(f'Fetched {len(rankings_df)} ranked teams. Upserting into BigQuery...')
    affected = games_query.upsert_to_bq(rankings_df, table_type='rankings')
    click.echo(f'Upsert completed successfully ({affected} rows affected).')


@cli.command()
@click.option('--date', type=click.DateTime(formats=['%Y-%m-%d']), default=None)
def sync_games(date: datetime.datetime | None = None):
    """Query and upsert both ESPN game metadata and scores in one command."""
    click.echo('Syncing ESPN game metadata and scores...')
    games_query = GamesQuery(api_key=my_api_key, gcp_service_accnt=gcp_service_accnt)
    dt = date.date() if date else None

    metadata_df = games_query.query_game_metadata(date=dt)
    click.echo(f'Fetched {len(metadata_df)} metadata rows.')
    meta_affected = games_query.upsert_to_bq(metadata_df, table_type='metadata')

    scores_df = games_query.query_game_scores(date=dt)
    click.echo(f'Fetched {len(scores_df)} score rows.')
    scores_affected = games_query.upsert_to_bq(scores_df, table_type='scores')

    click.echo(
        f'Sync complete. Metadata: {meta_affected} rows affected, Scores: {scores_affected} rows affected.'
    )


cli.add_command(get_new_odds)
cli.add_command(write_odds_to_bq)
cli.add_command(get_game_metadata)
cli.add_command(get_game_scores)
cli.add_command(get_ncaa_rankings)
cli.add_command(sync_games)

if __name__ == '__main__':
    cli()


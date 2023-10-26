"""
Create CLICK cli file for odds tracking
"""


import click
from app.query_betting_api import OddsQuery
from app.gcs_to_bigquery import GcsToBq
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


cli.add_command(get_new_odds)
cli.add_command(write_odds_to_bq)


if __name__ == '__main__':
    cli()

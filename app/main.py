"""
Create CLICK cli file for odds tracking
"""


import click
from query_betting_api import OddsQuery
from config.config import my_api_key, gcp_service_accnt


@click.group()
def cli():
    pass


@cli.command()
@click.argument('test_mode',
                default=False)  # ,
# help='Boolean value to limit the number of API calls')
def get_new_odds(test_mode):
    """
    Script to pull next Sunday's NFL betting odds.
    The OddsQuery class does the work of querying the betting API
    and writing results to GCS
    """
    click.echo('getting next weeks NFL betting odds')

    odds_query = OddsQuery(
        api_key=my_api_key, gcp_service_accnt=gcp_service_accnt)
    odds_data = odds_query.get_new_odds(test_mode=test_mode)
    odds_query.write_to_gcs(betting_data=odds_data)


@cli.command()
def refresh_db():
    click.echo('refreshing odds table with new CGS data')


cli.add_command(get_new_odds)
cli.add_command(refresh_db)


if __name__ == '__main__':
    cli()

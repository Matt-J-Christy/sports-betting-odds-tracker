"""
Move odds data from CGS to BigQuery
"""

from google.cloud import storage, bigquery
from google.oauth2 import service_account
import sys
import os
import io
import pandas as pd
from typing import Optional

sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from app.config.config import gcp_service_accnt  # noqa: E402
from app.ddl.table_schemas import odds_schema  # noqa: E402


class GcsToBq():
    """
    Creating class to query latest GCS odds data and
    write to the BQ table `odds-tracker-402301.nfl_data.daily_odds_data`
    """

    def __init__(
        self,
        gcp_service_accnt: service_account.Credentials,
        gcs_bucket: str,
        table_id: str
    ) -> None:

        self.gcp_service_accnt = gcp_service_accnt
        self.gcs_bucket = gcs_bucket
        self.table_id = table_id

    def get_latest_gcs_data(self, date: Optional[str] = None) -> pd.DataFrame:

        storage_client = storage.Client(credentials=self.gcp_service_accnt)
        bucket = storage_client.get_bucket(self.gcs_bucket)

        if date is not None:
            blob_name = 'betting_odds_' + date + '/data.csv'
            new_blob = bucket.blob(blob_name=blob_name)
            print('getting data from gcs bucket:', blob_name)
        else:
            items = []
            for blob in bucket.list_blobs(prefix='betting'):
                items.append(str(blob.name))
            items.sort(reverse=True)
            last_data = items[0]
            print('getting data from gcs bucket:', last_data)
            new_blob = bucket.blob(blob_name=last_data)

        blob_bytes = new_blob.download_as_bytes()
        df = pd.read_csv(io.BytesIO(blob_bytes))
        df['update_date'] = pd.to_datetime(df['update_date'])

        return df

    def write_to_bq(self, odds_data: pd.DataFrame) -> None:
        """
        read in most recent data from BQ table and insert values
        where the update day is > latest update day for the table
        """

        bq = bigquery.Client(credentials=self.gcp_service_accnt)

        project = 'odds-tracker-402301'
        dataset_id = 'nfl_data'
        table_id = 'daily_odds_data'

        sql = """
            with data_order as (
            select *,
                row_number()
                    over(partition by game_id,
                        value order by update_date desc) as row_num
            from `{}.{}.{}`
            )

            select *
            from data_order
            where row_num = 1;
        """.format(
            project, dataset_id, table_id
        )

        current_table = bq.query(sql).to_dataframe()
        current_table['update_day'] = pd.to_datetime(
            current_table['update_date']).dt.date

        odds_data['update_day'] = odds_data['update_date'].dt.date

        merged_df = odds_data.merge(current_table[['bet_id', 'game_id', 'value',
                                                   'update_date', 'update_day']],
                                    how='left',
                                    on=['game_id', 'bet_id',
                                        'value', 'update_day'],
                                    suffixes=(None, '_gcp')
                                    )

        cols = [
            'value', 'odd', 'bet_subgroup',
            'subgroup_value', 'bet_id', 'bet_name',
            'bookmaker_id', 'book', 'game_id', 'update_date'
        ]

        new_data = merged_df.loc[merged_df['update_date_gcp'].isna(), cols]

        if new_data.shape[0] > 0:

            job_config = bigquery.LoadJobConfig(
                schema=odds_schema
            )

            job = bq.load_table_from_dataframe(
                new_data,
                self.table_id,
                job_config=job_config
            )

            print(job.result)
            print('Writing {} rows to table'.format(new_data.shape[0]))
        else:
            print('Table already contains data from', str(
                odds_data['update_day'].unique()[0]))


if __name__ == '__main__':
    gcs_to_bq = GcsToBq(gcp_service_accnt=gcp_service_accnt,
                        gcs_bucket='odds-data',
                        table_id='odds-tracker-402301.nfl_data.daily_odds_data'
                        )
    gcs_data = gcs_to_bq.get_latest_gcs_data()
    gcs_to_bq.write_to_bq(odds_data=gcs_data)

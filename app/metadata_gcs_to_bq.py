"""
Move game metadata data from CGS to BigQuery
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
from app.ddl.table_schemas import game_metadata_schema  # noqa: E402


class GameMetadataToBq():
    """
    Creating class to query latest GCS odds data and
    write to the BQ table `odds-tracker-402301.nfl_data.game_metadata`
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
            blob_name = 'game_data_' + date + '/game_metadata.csv'
            new_blob = bucket.blob(blob_name=blob_name)
            print('getting data from gcs bucket:', blob_name)
        else:
            items = []
            for blob in bucket.list_blobs(prefix='game_data'):
                if 'metadata' in blob.name:
                    items.append(str(blob.name))
            items.sort(reverse=True)
            last_data = items[0]
            print('getting data from gcs bucket:', last_data)
            new_blob = bucket.blob(blob_name=last_data)

        blob_bytes = new_blob.download_as_bytes()
        df = pd.read_csv(io.BytesIO(blob_bytes))
        df['game_date'] = pd.to_datetime(df['game_date'])

        return df

    def write_to_bq(self, metadata_df: pd.DataFrame) -> None:
        """
        read in most recent data from BQ table and insert values
        where the update day is > latest update day for the table
        """

        bq = bigquery.Client(credentials=self.gcp_service_accnt)

        project = 'odds-tracker-402301'
        dataset_id = 'nfl_data'
        table_id = 'game_metadata'

        sql = """
            select game_id
            from `{}.{}.{}`
        """.format(
            project, dataset_id, table_id
        )

        current_table = bq.query(sql).to_dataframe()

        new_data = metadata_df.loc[~metadata_df['game_id'].isin(
            current_table['game_id']), :]

        if new_data.shape[0] > 0:

            job_config = bigquery.LoadJobConfig(
                schema=game_metadata_schema
            )

            job = bq.load_table_from_dataframe(
                new_data,
                self.table_id,
                job_config=job_config
            )

            print(job.result)
            print('Writing {} rows to table'.format(new_data.shape[0]))
        else:
            print('Metadata for games alread exists')


if __name__ == '__main__':
    gcs_to_bq = GameMetadataToBq(gcp_service_accnt=gcp_service_accnt,
                                 gcs_bucket='odds-data',
                                 table_id='odds-tracker-402301.nfl_data.game_metadata'
                                 )
    gcs_data = gcs_to_bq.get_latest_gcs_data()
    gcs_to_bq.write_to_bq(metadata_df=gcs_data)

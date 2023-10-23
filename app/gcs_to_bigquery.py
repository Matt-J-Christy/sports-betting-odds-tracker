"""
Move odds data from CGS to BigQuery
"""


from google.cloud import storage, bigquery
from google.oauth2 import service_account
import sys
import os
import io
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from app.config.config import gcp_service_accnt

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

    def get_latest_gcs_data(self) -> pd.DataFrame:

        storage_client = storage.Client(credentials=self.gcp_service_accnt)
        bucket = storage_client.get_bucket(self.gcs_bucket)

        items = []
        for blob in bucket.list_blobs(prefix='betting'):
            items.append(str(blob.name))
            print(str(blob.name))

        items.sort(reverse=True)
        last_data = items[0]

        new_blob = bucket.blob(blob_name=last_data)

        blob_bytes = new_blob.download_as_bytes()
        df = pd.read_csv(io.BytesIO(blob_bytes))
        df['update_date'] = pd.to_datetime(df['update_date'])

        return df

    def write_to_bq(self, odds_data) -> None:

        bq = bigquery.Client(credentials=self.gcp_service_accnt)

        schema = [
            bigquery.SchemaField("value", "STRING"),
            bigquery.SchemaField("odd", "FLOAT64"),
            bigquery.SchemaField("bet_id", "INT64"),
            bigquery.SchemaField("bet_name", "STRING"),
            bigquery.SchemaField("bookmaker_id", "INT64"),
            bigquery.SchemaField("book", "STRING"),
            bigquery.SchemaField("game_id", "INT64"),
            bigquery.SchemaField("update_date", "DATETIME")
        ]

        job_config = bigquery.LoadJobConfig(
            schema=schema
        )

        job = bq.load_table_from_dataframe(
            odds_data,
            self.table_id,
            job_config=job_config
        )

        print(job.result)
        print('Number of rows writting to table:', odds_data[0])


if __name__ == '__main__':
    gcs_to_bq = GcsToBq(gcp_service_accnt=gcp_service_accnt,
                       gcs_bucket='odds-data',
                       table_id='odds-tracker-402301.nfl_data.daily_odds_data'
                    )
    gcs_data = gcs_to_bq.get_latest_gcs_data()
    gcs_to_bq.write_to_bq(odds_data=gcs_data)
    


"""
//
Erg. Get blob items
//
"""


from google.cloud import storage, bigquery
from google.oauth2 import service_account
import sys
import os
import io
import pandas as pd
from app.config.config import gcp_service_accnt


storage_client = storage.Client(credentials=gcp_service_accnt)
bucket = storage_client.get_bucket('odds-data')

items = []
for blob in bucket.list_blobs(prefix='betting'):
    items.append(str(blob.name))
    print(str(blob.name))

items.sort(reverse=True)
last_data = items[0]

new_blob = bucket.blob(blob_name=items[0])

blob_bytes = new_blob.download_as_bytes()

df = pd.read_csv(io.BytesIO(blob_bytes))
df['update_date'] = pd.to_datetime(df['update_date'])

"""Move odds data from GCS to BigQuery."""

from __future__ import annotations

import datetime
import io
import os
import sys

import pandas as pd
from google.cloud import bigquery, storage
from google.oauth2 import service_account

sys.path.append(os.path.join(os.path.dirname(__file__), '../'))
from app.config.config import gcp_service_accnt  # noqa: E402
from app.ddl.table_schemas import odds_schema  # noqa: E402


class GcsToBq:
    """Load the newest odds CSV file from GCS into the odds table."""

    def __init__(
        self,
        gcp_service_accnt: service_account.Credentials | None,
        gcs_bucket: str,
        table_id: str,
    ) -> None:
        self.gcp_service_accnt = gcp_service_accnt
        self.gcs_bucket = gcs_bucket
        self.table_id = table_id

    def get_latest_gcs_data(self, date: str | None = None) -> pd.DataFrame:
        storage_client = storage.Client(credentials=self.gcp_service_accnt)
        bucket = storage_client.get_bucket(self.gcs_bucket)

        if date is not None:
            blob_name = "betting_odds_" + date + "/data.csv"
            new_blob = bucket.blob(blob_name=blob_name)
            print("getting data from gcs bucket:", blob_name)
        else:
            items = [str(blob.name) for blob in bucket.list_blobs(prefix="betting")]
            items.sort(reverse=True)
            if not items:
                raise FileNotFoundError("No betting odds CSVs found in the GCS bucket.")
            last_data = items[0]
            print("getting data from gcs bucket:", last_data)
            new_blob = bucket.blob(blob_name=last_data)

        blob_bytes = new_blob.download_as_bytes()
        df = pd.read_csv(io.BytesIO(blob_bytes))

        if "snapshot_time" not in df.columns:
            df["snapshot_time"] = pd.Timestamp.now(datetime.UTC).isoformat()
        if "commence_time" not in df.columns:
            df["commence_time"] = pd.NaT

        return df

    def write_to_bq(self, odds_data: pd.DataFrame) -> None:
        bq = bigquery.Client(credentials=self.gcp_service_accnt)

        if odds_data.empty:
            print("No new odds rows to load.")
            return

        project, dataset_id, table_name = self.table_id.split(".")
        target_table = f"`{project}.{dataset_id}.{table_name}`"

        # Check if table exists; if so, skip exact duplicate snapshot_times to prevent double-counting
        try:
            snapshot_times = odds_data["snapshot_time"].dropna().unique().tolist()
            if snapshot_times:
                quoted_times = ", ".join(f"TIMESTAMP('{t}')" for t in snapshot_times)
                existing_check = bq.query(
                    f"SELECT DISTINCT snapshot_time FROM {target_table} "
                    f"WHERE snapshot_time IN ({quoted_times})"
                ).to_dataframe()
                if not existing_check.empty:
                    existing_times = set(existing_check["snapshot_time"].astype(str))
                    odds_data = odds_data[~odds_data["snapshot_time"].astype(str).isin(existing_times)]
        except Exception as err:
            # Table might not exist yet or empty; proceed to create/load
            print(f"Snapshot deduplication check skipped ({err}). Appending rows.")

        if odds_data.empty:
            print("Table already contains this exact snapshot timestamp.")
            return

        job_config = bigquery.LoadJobConfig(
            schema=odds_schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        job = bq.load_table_from_dataframe(odds_data, self.table_id, job_config=job_config)
        job.result()
        print(f"Successfully appended {odds_data.shape[0]} rows to table {self.table_id}")


if __name__ == "__main__":
    gcs_to_bq = GcsToBq(
        gcp_service_accnt=gcp_service_accnt,
        gcs_bucket="odds-data",
        table_id="odds-tracker-402301.nfl_data.daily_odds_data",
    )
    gcs_data = gcs_to_bq.get_latest_gcs_data()
    gcs_to_bq.write_to_bq(odds_data=gcs_data)

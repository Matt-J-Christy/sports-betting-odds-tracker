"""BigQuery table schemas for the current odds tracker pipeline."""

from google.cloud import bigquery

odds_schema = [
    bigquery.SchemaField("snapshot_time", "TIMESTAMP"),
    bigquery.SchemaField("game_id", "STRING"),
    bigquery.SchemaField("sport", "STRING"),
    bigquery.SchemaField("away_team", "STRING"),
    bigquery.SchemaField("home_team", "STRING"),
    bigquery.SchemaField("commence_time", "TIMESTAMP"),
    bigquery.SchemaField("market_type", "STRING"),
    bigquery.SchemaField("team_or_outcome", "STRING"),
    bigquery.SchemaField("price", "INT64"),
    bigquery.SchemaField("market_point", "FLOAT64"),
]

game_metadata_schema = [
    bigquery.SchemaField("event_id", "STRING"),
    bigquery.SchemaField("sport_key", "STRING"),
    bigquery.SchemaField("sport_title", "STRING"),
    bigquery.SchemaField("commence_time", "TIMESTAMP"),
    bigquery.SchemaField("home_team", "STRING"),
    bigquery.SchemaField("away_team", "STRING"),
]

game_score_schema = [
    bigquery.SchemaField("event_id", "STRING"),
    bigquery.SchemaField("sport_key", "STRING"),
    bigquery.SchemaField("sport_title", "STRING"),
    bigquery.SchemaField("commence_time", "TIMESTAMP"),
    bigquery.SchemaField("completed", "BOOL"),
    bigquery.SchemaField("last_update", "TIMESTAMP"),
    bigquery.SchemaField("home_team", "STRING"),
    bigquery.SchemaField("away_team", "STRING"),
    bigquery.SchemaField("home_score", "INT64"),
    bigquery.SchemaField("away_score", "INT64"),
]

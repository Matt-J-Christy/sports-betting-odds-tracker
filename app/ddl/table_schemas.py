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
    bigquery.SchemaField("game_id", "INT64"),
    bigquery.SchemaField("game_stage", "STRING"),
    bigquery.SchemaField("week", "INT64"),
    bigquery.SchemaField("game_date", "DATE"),
    bigquery.SchemaField("game_time_utc", "STRING"),
    bigquery.SchemaField("city", "STRING"),
    bigquery.SchemaField("home_team_id", "INT64"),
    bigquery.SchemaField("home_team", "STRING"),
    bigquery.SchemaField("away_team_id", "INT64"),
    bigquery.SchemaField("away_team", "STRING"),
]

game_score_schema = [
    bigquery.SchemaField("game_id", "INT64"),
    bigquery.SchemaField("home_team_id", "INT64"),
    bigquery.SchemaField("away_team_id", "INT64"),
    bigquery.SchemaField("home_score_q1", "INT64"),
    bigquery.SchemaField("home_score_q2", "INT64"),
    bigquery.SchemaField("home_score_q3", "INT64"),
    bigquery.SchemaField("home_score_q4", "INT64"),
    bigquery.SchemaField("home_score_final", "INT64"),
    bigquery.SchemaField("away_score_q1", "INT64"),
    bigquery.SchemaField("away_score_q2", "INT64"),
    bigquery.SchemaField("away_score_q3", "INT64"),
    bigquery.SchemaField("away_score_q4", "INT64"),
    bigquery.SchemaField("away_score_final", "INT64"),
]

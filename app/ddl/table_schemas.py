"""
Define schemas for the three API queries
1. Odds table
2. Game metadata
3. Game scores

These schemas are used as and input into the
gcs_to_bigquery class to move data from gcs to BQ
"""

from google.cloud import bigquery

odds_schema = [
    bigquery.SchemaField("value", "STRING"),
    bigquery.SchemaField("odd", "FLOAT64"),
    bigquery.SchemaField("bet_subgroup", "STRING"),
    bigquery.SchemaField("subgroup_value", "FLOAT64"),
    bigquery.SchemaField("bet_id", "INT64"),
    bigquery.SchemaField("bet_name", "STRING"),
    bigquery.SchemaField("bookmaker_id", "INT64"),
    bigquery.SchemaField("book", "STRING"),
    bigquery.SchemaField("game_id", "INT64"),
    bigquery.SchemaField("update_date", "DATETIME")
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
    bigquery.SchemaField("away_team", "STRING")
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

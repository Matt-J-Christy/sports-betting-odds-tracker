# V1 Deployment Progress

Updated: 2026-09-10

## Current deployment

- Google Cloud project: `odds-tracker-402301`
- Region: `us-central1`
- Cloud Run service: `sports-odds-collector`
- Active revision: `sports-odds-collector-00006-bmf`
- Traffic: 100% on the active revision
- Scheduler job: `odds-collector-four-hourly`
- Scheduler cadence: every four hours, authenticated with OIDC
- BigQuery dataset: `odds-tracker-402301.nfl_data`

## Data pipeline

- The Odds API provides NFL and NCAA Football sportsbook snapshots.
- ESPN scoreboard endpoints provide game metadata and scores through the local CLI path.
- Canonical schemas are defined in `app/ddl/table_schemas.py`.
- Table provisioning uses `scripts/preview_bigquery_tables.py` and the guarded destructive reset in `scripts/reset_bigquery_tables.py`.
- API credentials are loaded from `.env` locally and Secret Manager in Cloud Run.
- Cloud Run errors redact API keys before returning or logging messages.

## Verified results

- The three production tables were dropped and recreated with the canonical schemas.
- Secret Manager version `4` is enabled and accepted by The Odds API.
- A direct authenticated Cloud Run smoke test returned `status: success`.
- The smoke test made successful NFL and NCAA requests and ingested 438 rows.
- The odds table contains 876 rows after two successful ingestion calls.
- Representative odds rows were verified in `daily_odds_data`.
- `game_metadata` and `game_results` remain empty because the scheduled Cloud Run collector currently ingests odds only; ESPN metadata and scores are available through the CLI commands.

## Validation

- Full test suite: 65 tests passed.
- Focused ESPN, schema, reset, and ingestion tests passed.
- Changed deployment files pass Ruff checks.
- Shell syntax and whitespace checks passed.

## Operational commands

Run the production smoke test through Scheduler:

```bash
gcloud scheduler jobs run odds-collector-four-hourly --location us-central1
```

Check recent Cloud Run logs:

```bash
gcloud run services logs read sports-odds-collector \
  --region us-central1 \
  --limit=50
```

Check BigQuery row counts:

```bash
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) AS row_count
   FROM \`odds-tracker-402301.nfl_data.daily_odds_data\`"
```

## Remaining work

- Decide whether ESPN metadata and scores should be added to the scheduled Cloud Run handler or remain CLI-operated.
- Keep the local `.env` key and Secret Manager version synchronized when credentials rotate.
- Preserve `README.md` and `PRODUCTION_DEPLOYMENT.md` as the active V1 documentation.

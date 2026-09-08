# Sports Betting Odds Tracker – Progress Notes

Date: 2026-08-31

## Migration plan status
- Security / secret handling: ✅ Complete
  - API keys are now loaded from environment variables via `.env` and the project no longer relies on hardcoded secrets in source files.
  - Repo history review did not reveal a raw committed key value in the current project history.
- Remaining migration work: 🔄 In progress
  - Modernize the odds API consumer to the current The Odds API response shape.
  - Reconcile the schema and ETL pipeline with the FanDuel-style snapshot format.
  - Validate the local app and data pipeline end-to-end with a working dev environment.

## Current goal
Refactor the project in place without scrapping the existing ETL work. The short-term goals are:
1. Use the newer The Odds API pattern instead of the stale API-Sports endpoints.
2. Update the data processing and BigQuery schema to the current FanDuel-style format.
3. Add light app/UI scaffolding so the project is usable again without a full rewrite.

## What was learned
- The repository was originally built against the old `api-sports.io` API and stale `gcp_service_accnt` / config assumptions.
- The repo update docs in `repo_updates/` point toward a simpler FanDuel-style odds pipeline and a safer secret-management pattern.
- The project already contains the general ETL structure (API fetchers, GCS staging, BigQuery loaders), but much of the code is tied to old schemas and API shapes.

## Major changes made
- Updated the odds API fetch layer in `app/query_betting_api.py` to use the newer The Odds API endpoint and flatten the current bookmaker/market/outcome payload.
- Added a safer env-based configuration module at `app/config/config.py` to load secrets from `.env` and local credentials without hard-coded values.
- Updated the BigQuery schema definitions in `app/ddl/table_schemas.py` to a current snapshot-style format:
  - `snapshot_time`
  - `game_id`
  - `sport`
  - `away_team`
  - `home_team`
  - `commence_time`
  - `market_type`
  - `team_or_outcome`
  - `price`
  - `market_point`
- Added a minimal Streamlit scaffold in `streamlit/streamlit_app.py` that renders a simple sample odds dashboard without depending on a live GCP connection.

## Key files relevant to the next session
- `app/query_betting_api.py`
- `app/config/config.py`
- `app/ddl/table_schemas.py`
- `app/gcs_to_bigquery.py`
- `app/ddl/daily_odds_data_ddl.py`
- `streamlit/streamlit_app.py`
- `repo_updates/fanduel_tracker_reference.md`
- `repo_updates/gcp_security_guide.md`

## Validation status
- Verified by direct Python AST parse that the touched files are syntactically valid.
- This is not yet a full runtime pass because the project does not currently have a complete Python environment setup or live API/GCP credentials in the workspace.

## Next recommended continuation
1. Install the project dependencies in a working virtual environment or local interpreter.
2. Validate one real API call to The Odds API with a local `.env` key.
3. Confirm the data shape being returned and compare it to the BigQuery schema.
4. Test the local load to GCS and then to BigQuery against a single sample payload.
5. Reconnect the app UI to live data after ETL is confirmed.

## Quick notes
- `THE_ODDS_API_KEY` should be stored locally in `.env` and kept out of git.
- Do not rework the whole codebase yet; the current strategy is incremental modernization.
- Keep the old ETL flow mostly intact while adapting the API and schema assumptions.
- Link to the odds swagger docs: https://app.swaggerhub.com/apis-docs/the-odds-api/odds-api/4?view=uiDocs#/current%20events/get_v4_sports__sport__odds
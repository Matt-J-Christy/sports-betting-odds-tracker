#!/bin/bash
# =============================================================================
# Deploy Cloud Scheduler Jobs: 3-Job Architecture (100% GCP Always Free Tier)
# =============================================================================
set -e

PROJECT_ID="odds-tracker-402301"
LOCATION="us-central1"
SERVICE_URL="https://sports-odds-collector-273gwavvyq-uc.a.run.app"
SERVICE_ACCOUNT="odds-collector-scheduler@${PROJECT_ID}.iam.gserviceaccount.com"

echo "Configuring Cloud Scheduler jobs for project: ${PROJECT_ID} (region: ${LOCATION})"

# 1. Delete obsolete jobs if they exist
echo "Cleaning up obsolete jobs to preserve free quota..."
gcloud scheduler jobs delete game-scores-daily --location="${LOCATION}" --quiet 2>/dev/null || true
gcloud scheduler jobs delete game-metadata-bi-daily --location="${LOCATION}" --quiet 2>/dev/null || true

# 2. Four-hourly Odds Collection
echo "Configuring job: odds-collector-four-hourly..."
gcloud scheduler jobs create http odds-collector-four-hourly \
  --project="${PROJECT_ID}" \
  --location="${LOCATION}" \
  --schedule="0 */4 * * *" \
  --time-zone="America/New_York" \
  --uri="${SERVICE_URL}/" \
  --http-method="POST" \
  --oidc-service-account-email="${SERVICE_ACCOUNT}" \
  --oidc-token-audience="${SERVICE_URL}" \
  --description="Four-hourly betting odds snapshot collection from The Odds API" \
  || gcloud scheduler jobs update http odds-collector-four-hourly \
      --project="${PROJECT_ID}" \
      --location="${LOCATION}" \
      --schedule="0 */4 * * *" \
      --time-zone="America/New_York" \
      --uri="${SERVICE_URL}/" \
      --http-method="POST" \
      --oidc-service-account-email="${SERVICE_ACCOUNT}" \
      --oidc-token-audience="${SERVICE_URL}" \
      --description="Four-hourly betting odds snapshot collection from The Odds API"

# 3. Consolidated Daily Games Sync (Metadata + Scores)
echo "Configuring job: games-sync-daily..."
gcloud scheduler jobs create http games-sync-daily \
  --project="${PROJECT_ID}" \
  --location="${LOCATION}" \
  --schedule="0 4 * * 1,4,5,6,0" \
  --time-zone="America/New_York" \
  --uri="${SERVICE_URL}/games" \
  --http-method="POST" \
  --oidc-service-account-email="${SERVICE_ACCOUNT}" \
  --oidc-token-audience="${SERVICE_URL}" \
  --description="Daily ESPN game metadata and scores sync on Thu, Fri, Sat, Sun, and Mon" \
  || gcloud scheduler jobs update http games-sync-daily \
      --project="${PROJECT_ID}" \
      --location="${LOCATION}" \
      --schedule="0 4 * * 1,4,5,6,0" \
      --time-zone="America/New_York" \
      --uri="${SERVICE_URL}/games" \
      --http-method="POST" \
      --oidc-service-account-email="${SERVICE_ACCOUNT}" \
      --oidc-token-audience="${SERVICE_URL}" \
      --description="Daily ESPN game metadata and scores sync on Thu, Fri, Sat, Sun, and Mon"

# 4. Weekly NCAA Rankings Collection (AP Top 25)
echo "Configuring job: ncaa-rankings-weekly..."
gcloud scheduler jobs create http ncaa-rankings-weekly \
  --project="${PROJECT_ID}" \
  --location="${LOCATION}" \
  --schedule="0 15 * * 0" \
  --time-zone="America/New_York" \
  --uri="${SERVICE_URL}/rankings" \
  --http-method="POST" \
  --oidc-service-account-email="${SERVICE_ACCOUNT}" \
  --oidc-token-audience="${SERVICE_URL}" \
  --description="Weekly NCAA AP Top 25 rankings collection from ESPN on Sunday afternoons" \
  || gcloud scheduler jobs update http ncaa-rankings-weekly \
      --project="${PROJECT_ID}" \
      --location="${LOCATION}" \
      --schedule="0 15 * * 0" \
      --time-zone="America/New_York" \
      --uri="${SERVICE_URL}/rankings" \
      --http-method="POST" \
      --oidc-service-account-email="${SERVICE_ACCOUNT}" \
      --oidc-token-audience="${SERVICE_URL}" \
      --description="Weekly NCAA AP Top 25 rankings collection from ESPN on Sunday afternoons"

echo "Done. Exactly 3 jobs configured (100% within GCP Always Free Tier)."

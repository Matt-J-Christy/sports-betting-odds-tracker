#!/usr/bin/env python3
"""Provision and configure Cloud Scheduler jobs for odds, games sync, and NCAA rankings.

Enforces a strict 3-job maximum to remain 100% within the GCP Always Free Tier.
"""

import google.auth
import google.auth.transport.requests
import requests

PROJECT_ID = "odds-tracker-402301"
LOCATION = "us-central1"
BASE_URL = f"https://cloudscheduler.googleapis.com/v1/projects/{PROJECT_ID}/locations/{LOCATION}/jobs"
SERVICE_URI = "https://sports-odds-collector-273gwavvyq-uc.a.run.app"
SERVICE_ACCOUNT = f"odds-collector-scheduler@{PROJECT_ID}.iam.gserviceaccount.com"

# The 3 production jobs (exactly 3 jobs to stay 100% within GCP Free Tier)
TARGET_JOBS = [
    {
        "id": "odds-collector-four-hourly",
        "description": "Four-hourly betting odds snapshot collection from The Odds API",
        "schedule": "0 */4 * * *",
        "timeZone": "America/New_York",
        "endpoint": "/",
    },
    {
        "id": "games-sync-daily",
        "description": "Daily ESPN game metadata and scores sync on Thu, Fri, Sat, Sun, and Mon",
        "schedule": "0 4 * * 1,4,5,6,0",
        "timeZone": "America/New_York",
        "endpoint": "/games",
    },
    {
        "id": "ncaa-rankings-weekly",
        "description": "Weekly NCAA AP Top 25 rankings collection from ESPN on Sunday afternoons",
        "schedule": "0 15 * * 0",
        "timeZone": "America/New_York",
        "endpoint": "/rankings",
    },
]

OBSOLETE_JOB_IDS = ["game-scores-daily", "game-metadata-bi-daily"]


def get_auth_headers():
    credentials, _ = google.auth.default()
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    return {
        "Authorization": f"Bearer {credentials.token}",
        "Content-Type": "application/json",
    }


def deploy_jobs():
    headers = get_auth_headers()
    print("Checking existing Cloud Scheduler jobs...")
    resp = requests.get(BASE_URL, headers=headers)
    existing_jobs = {}
    if resp.status_code == 200:
        for j in resp.json().get("jobs", []):
            job_name = j.get("name", "").split("/")[-1]
            existing_jobs[job_name] = j
        print(f"Currently active jobs ({len(existing_jobs)}): {list(existing_jobs.keys())}")
    else:
        print(f"Error fetching jobs [{resp.status_code}]: {resp.text}")
        return

    # 1. Clean up obsolete jobs
    for obs_id in OBSOLETE_JOB_IDS:
        if obs_id in existing_jobs:
            print(f"Deleting obsolete job '{obs_id}' to preserve free tier quota...")
            del_url = f"{BASE_URL}/{obs_id}"
            del_resp = requests.delete(del_url, headers=headers)
            if del_resp.status_code == 200:
                print(f"  Successfully deleted '{obs_id}'.")
                existing_jobs.pop(obs_id, None)
            else:
                print(f"  Failed to delete '{obs_id}': {del_resp.text}")

    # 2. Deploy target jobs
    for job_def in TARGET_JOBS:
        job_id = job_def["id"]
        job_name = f"projects/{PROJECT_ID}/locations/{LOCATION}/jobs/{job_id}"
        payload = {
            "name": job_name,
            "description": job_def["description"],
            "schedule": job_def["schedule"],
            "timeZone": job_def["timeZone"],
            "httpTarget": {
                "uri": f"{SERVICE_URI}{job_def['endpoint']}",
                "httpMethod": "POST",
                "headers": {"User-Agent": "Google-Cloud-Scheduler"},
                "oidcToken": {
                    "serviceAccountEmail": SERVICE_ACCOUNT,
                    "audience": SERVICE_URI,
                },
            },
        }

        if job_id in existing_jobs:
            print(f"Updating existing job '{job_id}'...")
            update_url = f"{BASE_URL}/{job_id}?updateMask=description,schedule,timeZone,httpTarget"
            up_resp = requests.patch(update_url, headers=headers, json=payload)
            if up_resp.status_code == 200:
                print(f"  Successfully updated '{job_id}'.")
            else:
                print(f"  Failed to update '{job_id}' [{up_resp.status_code}]: {up_resp.text}")
        else:
            print(f"Creating new job '{job_id}'...")
            create_resp = requests.post(BASE_URL, headers=headers, json=payload)
            if create_resp.status_code == 200:
                print(f"  Successfully created '{job_id}'.")
            else:
                print(f"  Failed to create '{job_id}' [{create_resp.status_code}]: {create_resp.text}")


if __name__ == "__main__":
    deploy_jobs()

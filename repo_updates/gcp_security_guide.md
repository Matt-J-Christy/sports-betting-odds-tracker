# GCP Security Reference: Secret Management & Key Rotation
This reference document outlines the steps required to secure your **sports-betting-odds-tracker** application by moving hardcoded keys into **Google Cloud Secret Manager**.

---

## Step 1: Add the Secret to GCP Secret Manager
Execute these commands in your terminal using the `gcloud` CLI to enable the service and securely store your API token.

```bash
# 1. Enable the Secret Manager API
gcloud services enable secretmanager.googleapis.com

# 2. Create the secret container
gcloud secrets create THE_ODDS_API_KEY --replication-policy="automatic"

# 3. Add your actual API key as the first version
echo -n "YOUR_ACTUAL_API_KEY_HERE" | gcloud secrets versions add THE_ODDS_API_KEY --data-file=-
```

---

## Step 2: Grant Permissions to Cloud Run
Cloud Run uses a runtime service account identity. You must authorize this identity to access the secret container.

1. Open the **Secret Manager** dashboard in the GCP Console.
2. Select your secret named `THE_ODDS_API_KEY`.
3. Navigate to the **Permissions** tab on the right side panel and click **Add Principal**.
4. In the **New Principals** field, paste your Cloud Run runtime service account email. 
   *(By default, this is the Compute Engine service account: `PROJECT_NUMBER-compute@developer.gserviceaccount.com`)*
5. Assign the role: **Secret Manager Secret Accessor** (`roles/secretmanager.secretAccessor`).
6. Click **Save**.

---

## Step 3: Map the Secret into Cloud Run Config
Instead of writing complex code to fetch secrets programmatically, mount the secret directly as an environment variable in Cloud Run. This lets you continue using standard `os.getenv()` statements natively in Python.

Deploy or update your service using the `--set-secrets` flag:

```bash
gcloud run deploy sports-odds-collector \
  --source . \
  --set-secrets="THE_ODDS_API_KEY=THE_ODDS_API_KEY:latest"
```

---

## Step 4: Handle Local Development Securely
To prevent accidental commits when debugging your app on your local machine, use a `.env` file that is explicitly ignored by your version control system.

### 1. Create a Local Environment File (`.env`)
Create this file in the root directory of your project:
```text
THE_ODDS_API_KEY=your_actual_local_development_key
```

### 2. Update Your Git Ignore (`.gitignore`)
Append the `.env` filename to your `.gitignore` rules to ensure it is never pushed to GitHub:
```text
# Local environment variables configuration secrets
.env
```

### 3. Load Variables via Python (`main.py` / `app.py`)
Install the lightweight python-dotenv package:
```bash
pip install python-dotenv
```
Update your application setup to securely load your secrets across both local environments and production Cloud Run targets:
```python
import os
from dotenv import load_dotenv

# Load variables from a local .env file if it exists
load_dotenv()

# This statement automatically functions seamlessly across both your 
# local .env config and Cloud Run's mapped environment variables
API_KEY = os.getenv("THE_ODDS_API_KEY")
```

---

## Step 5: Remediate Exposed Repository History
If an API key has already been pushed to a public repository like GitHub, simply deleting the line from your script does not secure it; the string remains accessible inside past Git commits.

1. **Invalidate immediately:** Log into *The Odds API* or *TheRundown* control center and click **Regenerate / Revoke Key** to permanently disable the exposed credential.
2. **Purge Git History (Optional):** Use `git-filter-repo` to scrub the file history completely clean across all past iterations before re-syncing downstream nodes:
   ```bash
   pip install git-filter-repo
   git filter-repo --invert-paths --path paths/to/exposed_file.py
   ```
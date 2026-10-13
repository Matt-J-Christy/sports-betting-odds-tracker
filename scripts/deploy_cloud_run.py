#!/usr/bin/env python3
"""Build and deploy updated container to Cloud Run using Cloud Build API."""

import io
import os
import tarfile
import time
from pathlib import Path

import google.auth
import google.auth.transport.requests
import requests
from google.cloud import storage

PROJECT_ID = "odds-tracker-402301"
REGION = "us-central1"
SERVICE_NAME = "sports-odds-collector"
BUCKET_NAME = f"run-sources-{PROJECT_ID}-{REGION}"
IMAGE_NAME = f"{REGION}-docker.pkg.dev/{PROJECT_ID}/cloud-run-source-deploy/{SERVICE_NAME}"
ROOT_DIR = Path(__file__).resolve().parents[1]


def create_source_tarball() -> bytes:
    """Package repository into a tar.gz archive excluding venvs and caches."""
    tar_stream = io.BytesIO()
    ignore_patterns = {
        "venv",
        ".venv",
        "odds",
        ".git",
        "__pycache__",
        ".pytest_cache",
        ".ruff_cache",
        "scratch",
    }

    with tarfile.open(fileobj=tar_stream, mode="w:gz") as tar:
        for file_path in ROOT_DIR.rglob("*"):
            rel_path = file_path.relative_to(ROOT_DIR)
            # Skip ignored directories
            if any(part in ignore_patterns for part in rel_path.parts):
                continue
            if file_path.is_file():
                tar.add(file_path, arcname=str(rel_path))

    tar_stream.seek(0)
    return tar_stream.read()


def deploy():
    credentials, _ = google.auth.default()
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    token = credentials.token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    print(f"Creating source archive from {ROOT_DIR}...")
    source_bytes = create_source_tarball()
    print(f"Source archive size: {len(source_bytes) / 1024:.1f} KB")

    timestamp = int(time.time())
    archive_name = f"{SERVICE_NAME}-{timestamp}.tar.gz"

    storage_client = storage.Client(project=PROJECT_ID, credentials=credentials)
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(archive_name)
    blob.upload_from_string(source_bytes, content_type="application/gzip")
    print(f"Uploaded source archive to gs://{BUCKET_NAME}/{archive_name}")

    build_payload = {
        "source": {
            "storageSource": {
                "bucket": BUCKET_NAME,
                "object": archive_name,
            }
        },
        "steps": [
            {
                "name": "gcr.io/cloud-builders/docker",
                "args": [
                    "build",
                    "-t",
                    f"{IMAGE_NAME}:{timestamp}",
                    "-t",
                    f"{IMAGE_NAME}:latest",
                    ".",
                ],
            }
        ],
        "images": [f"{IMAGE_NAME}:{timestamp}", f"{IMAGE_NAME}:latest"],
    }

    print("Submitting Cloud Build job...")
    build_url = f"https://cloudbuild.googleapis.com/v1/projects/{PROJECT_ID}/builds"
    build_resp = requests.post(build_url, headers=headers, json=build_payload)
    if build_resp.status_code != 200:
        print(f"Build submission failed [{build_resp.status_code}]: {build_resp.text}")
        return

    build_data = build_resp.json()
    build_id = build_data.get("metadata", {}).get("build", {}).get("id") or build_data.get("id")
    print(f"Cloud Build started with ID: {build_id}")

    # Poll build status
    status_url = f"https://cloudbuild.googleapis.com/v1/projects/{PROJECT_ID}/builds/{build_id}"
    while True:
        time.sleep(10)
        credentials.refresh(auth_req)
        headers["Authorization"] = f"Bearer {credentials.token}"
        poll_resp = requests.get(status_url, headers=headers)
        if poll_resp.status_code != 200:
            continue
        status = poll_resp.json().get("status")
        print(f"  Build status: {status}...")
        if status in ("SUCCESS", "FAILURE", "INTERNAL_ERROR", "TIMEOUT", "CANCELLED"):
            break

    if status != "SUCCESS":
        print(f"Build finished with error status: {status}")
        return

    print("Build succeeded! Updating Cloud Run service...")
    run_url = f"https://run.googleapis.com/v2/projects/{PROJECT_ID}/locations/{REGION}/services/{SERVICE_NAME}"
    get_run = requests.get(run_url, headers=headers)
    if get_run.status_code != 200:
        print(f"Failed to fetch Cloud Run service [{get_run.status_code}]: {get_run.text}")
        return

    service_data = get_run.json()
    containers = service_data.get("template", {}).get("containers", [{}])
    containers[0]["image"] = f"{IMAGE_NAME}:{timestamp}"

    patch_url = f"{run_url}?updateMask=template.containers"
    patch_payload = {
        "template": {
            "containers": containers,
        }
    }
    patch_resp = requests.patch(patch_url, headers=headers, json=patch_payload)
    if patch_resp.status_code == 200:
        print(f"Successfully updated Cloud Run service to image {IMAGE_NAME}:{timestamp}!")
    else:
        print(f"Cloud Run update failed [{patch_resp.status_code}]: {patch_resp.text}")


if __name__ == "__main__":
    deploy()


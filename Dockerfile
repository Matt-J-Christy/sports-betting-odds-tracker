# ========================================================
# Multi-Target Production Dockerfile
# Targets:
#   1. collector (DEFAULT): Cloud Run Ingestion Service
#   2. dashboard: Streamlit Analytics Dashboard
#
# Build the collector (default) via:
#   docker build -t odds-collector .
#   gcloud run deploy --source .
#
# Build the dashboard explicitly via:
#   docker build --target dashboard -t odds-dashboard .
# ========================================================

# --- Base Image ---
FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PORT=8080 \
    APP_HOME=/app

WORKDIR $APP_HOME

# Install curl for healthchecks
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create unprivileged runtime user
RUN groupadd -r appuser && useradd -r -g appuser -d $APP_HOME -s /sbin/nologin appuser

# Install base dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy application source code
COPY . .

# Ensure ownership by unprivileged user
RUN chown -R appuser:appuser $APP_HOME
USER appuser

# --- Target 2: Dashboard (Streamlit Analytics UI) ---
# Defined first so that the final (default) stage is the collector.
FROM base AS dashboard

USER root
RUN pip install streamlit plotly
USER appuser

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD curl --fail http://localhost:8501/_stcore/health || exit 1

CMD ["streamlit", "run", "streamlit/streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]

# --- Target 1: Collector (Default — Cloud Run Ingestion Service) ---
# This is the FINAL stage so a plain `docker build .` or
# `gcloud run deploy --source .` automatically builds the collector.
FROM base AS collector

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl --fail http://localhost:8080/health || exit 1

CMD ["python", "main.py"]

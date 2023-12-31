#!/bin/bash

# Script to run odds scraper and move
# data from GCS to BigQuery

echo "Running python CLI for new odds data"
python cli.py get-new-odds

echo "Using CLI to write new odds data from GCS to BigQuery"
python cli.py write-odds-to-bq

echo "Using CLI to get game metadata"
python cli.py get-game-metadata
python cli.py game-metadata-to-bq

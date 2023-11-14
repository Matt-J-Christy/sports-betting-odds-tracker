#!/bin/bash

# Script to get latest game results
# from API and write from GCS to BigQuery

echo "getting last week's game results"
python cli.py get-game-scores
python cli.py game-scores-to-bq

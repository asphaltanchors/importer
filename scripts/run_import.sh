#!/bin/bash
set -e

# Load environment variables
source /app/.env

# Run the importer
cd /app && poetry run importer import-csv --input-dir /data/input --log-level INFO

# Archive processed files
timestamp=$(date +%Y%m%d_%H%M%S)
mkdir -p /data/archive/$timestamp
mv /data/input/*.csv /data/archive/$timestamp/ 2>/dev/null || true

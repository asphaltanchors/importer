#!/bin/bash
set -e

# Run the importer with the mounted data directory and log output
cd /app && poetry run importer import-csv /data --log-level INFO >> /var/log/importer/import.log 2>&1

# Optional: Clean up old processed/failed files (keep last 30 days)
find /data/processed/* -type d -mtime +30 -exec rm -rf {} \; 2>/dev/null || true
find /data/failed/* -type d -mtime +30 -exec rm -rf {} \; 2>/dev/null || true

# Rotate logs if they get too large (keep last 100MB)
LOG_FILE="/data/logs/import.log"
if [ -f "$LOG_FILE" ] && [ $(stat --format=%s "$LOG_FILE") -gt 104857600 ]; then
    mv "$LOG_FILE" "${LOG_FILE}.1"
fi

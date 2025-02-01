#!/bin/sh
set -e

# Run initial import scan
echo "Running initial import scan..."
cd /app && node dist/process-daily-imports.js

# Start Supercronic for scheduled runs
echo "Starting scheduled import process..."
exec /usr/local/bin/supercronic /app/crontab

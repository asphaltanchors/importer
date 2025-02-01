#!/bin/bash
set -e

if [ $# -gt 0 ]; then
    # If arguments are passed, execute them
    exec "$@"
else
    # No arguments - first process existing files, then start cron
    echo "Processing existing CSV files..."
    /app/scripts/run_import.sh
    
    echo "Starting cron scheduler..."
    cd /app && exec /usr/local/bin/supercronic -debug /app/crontab.txt
fi

#!/bin/bash
# startup.sh

# Function to log messages
log_message() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" >> /var/log/csv_importer/import.log
}

# Process all existing files on startup
log_message "Starting initial import of existing files"
csv-import --input-dir /data/input --log-level INFO
initial_import_status=$?

if [ $initial_import_status -eq 0 ]; then
    log_message "Initial import completed successfully"
else
    log_message "Initial import failed with status $initial_import_status"
fi

# Start cron daemon
log_message "Starting cron daemon for scheduled imports"
cron

# Keep container running and tail logs
exec tail -f /var/log/csv_importer/import.log

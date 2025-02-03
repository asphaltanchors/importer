#!/bin/bash
set -e

# Set up logging
exec 1>> /var/log/importer/import.log 2>&1

echo "Starting import process at $(date)"

# Create today's processed/failed directories
TODAY=$(date +%Y-%m-%d)
mkdir -p "/data/processed/$TODAY" "/data/failed/$TODAY"

process_file() {
    local file="$1"
    local cmd="$2"
    local type="$3"
    
    echo "Processing $type file: $file"
    if poetry run python -m importer.cli $cmd "$file"; then
        echo "Successfully processed $file"
        mv "$file" "/data/processed/$TODAY/"
    else
        echo "Failed to process $file"
        mv "$file" "/data/failed/$TODAY/"
    fi
}

# Process customer files (excluding *_all.csv)
for f in /data/Customer_[0-9]*.csv; do
    if [ -f "$f" ]; then
        process_file "$f" "customers process" "customer"
    fi
done

# Process invoice files (excluding *_all.csv)
for f in /data/Invoice_[0-9]*.csv; do
    if [ -f "$f" ]; then
        process_file "$f" "sales process-invoices" "invoice"
    fi
done

# Process sales receipt files (excluding *_all.csv)
for f in "/data/Sales Receipt_[0-9]"*.csv; do
    if [ -f "$f" ]; then
        process_file "$f" "sales process-receipts" "sales receipt"
    fi
done

echo "Cleaning up old processed/failed files (older than 30 days)..."
find /data/processed/* -type d -mtime +30 -exec rm -rf {} \; 2>/dev/null || true
find /data/failed/* -type d -mtime +30 -exec rm -rf {} \; 2>/dev/null || true

# Rotate logs if they get too large (keep last 100MB)
LOG_FILE="/var/log/importer/import.log"
if [ -f "$LOG_FILE" ] && [ $(stat --format=%s "$LOG_FILE") -gt 104857600 ]; then
    mv "$LOG_FILE" "${LOG_FILE}.1"
fi

echo "Import process completed at $(date)"

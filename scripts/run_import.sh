#!/bin/bash
set -e

echo "Starting import process at $(date)"

# Create today's processed/failed directories
TODAY=$(date +%Y-%m-%d)
mkdir -p "/data/processed/$TODAY" "/data/failed/$TODAY"

# Function to count files matching a pattern
count_files() {
    local pattern="$1"
    local count=$(find /data -maxdepth 1 -name "$pattern" -type f | wc -l)
    echo "$count"
}

# Function to process files of a specific type
process_files() {
    local pattern="$1"
    local cmd="$2"
    local type="$3"
    
    local success_count=0
    local fail_count=0
    
    echo "Processing $type files..."
    find /data -maxdepth 1 -name "$pattern" -type f | while read -r f; do
        echo "  → Processing: $(basename "$f")"
        if importer $cmd "$f"; then
            echo "    ✓ Successfully processed $(basename "$f")"
            mv "$f" "/data/processed/$TODAY/"
            ((success_count++))
        else
            echo "    ✗ Failed to process $(basename "$f")"
            mv "$f" "/data/failed/$TODAY/"
            ((fail_count++))
        fi
    done
    
    echo "Completed processing $type files: $success_count succeeded, $fail_count failed"
    echo "----------------------------------------"
}

# Process customer files
count=$(count_files "Customer_[0-9]*.csv")
echo "Found $count customer files to process"
process_files "Customer_[0-9]*.csv" "customers process" "customer"

# Process invoice files
count=$(count_files "Invoice_[0-9]*.csv")
echo "Found $count invoice files to process"
process_files "Invoice_[0-9]*.csv" "process-invoices" "invoice"

# Process sales receipt files
count=$(count_files "Sales Receipt_[0-9]*.csv")
echo "Found $count sales receipt files to process"
process_files "Sales Receipt_[0-9]*.csv" "process-receipts" "sales receipt"

echo "Cleaning up old processed/failed files (older than 30 days)..."
find /data/processed/* -type d -mtime +30 -exec rm -rf {} \; 2>/dev/null || true
find /data/failed/* -type d -mtime +30 -exec rm -rf {} \; 2>/dev/null || true

echo "=== Import Summary ==="
echo "Processed files moved to: /data/processed/$TODAY/"
echo "Failed files moved to: /data/failed/$TODAY/"
echo "Import process completed at $(date)"

#!/bin/bash

# Run the Meltano pipeline for items only using the dedicated tap-csv-items plugin
# Using --full-refresh to ensure all data is loaded
echo "Running import for items from /Users/oren/Dropbox-AAC/AAC/Oren/CSV/item_all.csv"
echo "Step 1: Extract and load items data"
export ITEMS_FILE_PATH="/Users/oren/Dropbox-AAC/AAC/Oren/CSV/item_all.csv"
meltano run --full-refresh tap-csv-items target-postgres

# Run DBT to transform the data into the products table and track history
echo "Step 2: Transform items data into products table and track history"
meltano invoke dbt-postgres:run --models products item_history item_history_view

#!/bin/bash

export ITEMS_FILE_PATH="/Users/oren/Dropbox-AAC/AAC/Oren/CSV/item_all.csv"
meltano run --full-refresh tap-csv-items target-postgres

## Import the big set
export CUSTOMERS_FILE_PATH="/Users/oren/Dropbox-AAC/AAC/Oren/CSV/customers_all.csv"
export INVOICES_FILE_PATH="/Users/oren/Dropbox-AAC/AAC/Oren/CSV/invoice_all.csv"
export SALES_RECEIPTS_FILE_PATH="/Users/oren/Dropbox-AAC/AAC/Oren/CSV/sales_all.csv"

meltano run --full-refresh tap-csv target-postgres dbt-postgres:run

## Then the 90 day set
export CUSTOMERS_FILE_PATH="/Users/oren/Dropbox-AAC/AAC/Oren/CSV/customer_90.csv"
export INVOICES_FILE_PATH="/Users/oren/Dropbox-AAC/AAC/Oren/CSV/invoice_90.csv"
export SALES_RECEIPTS_FILE_PATH="/Users/oren/Dropbox-AAC/AAC/Oren/CSV/sales_90.csv"

# Run the Meltano pipeline
meltano run --full-refresh tap-csv target-postgres dbt-postgres:run

## Then run the matcher
./matcher.py


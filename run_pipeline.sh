#!/bin/bash

# Set file paths for Meltano pipeline
# Edit these paths as needed for your environment

# export CUSTOMERS_FILE_PATH="/Users/oren/Dropbox-AAC/AAC/Oren/CSV/customers_all.csv"
# export INVOICES_FILE_PATH="/Users/oren/Dropbox-AAC/AAC/Oren/CSV/invoice_all.csv"
# export SALES_RECEIPTS_FILE_PATH="/Users/oren/Dropbox-AAC/AAC/Oren/CSV/sales_all.csv"
export CUSTOMERS_FILE_PATH="/Users/oren/Dropbox-AAC/AAC/Oren/CSV/customer_90.csv"
export INVOICES_FILE_PATH="/Users/oren/Dropbox-AAC/AAC/Oren/CSV/invoice_90.csv"
export SALES_RECEIPTS_FILE_PATH="/Users/oren/Dropbox-AAC/AAC/Oren/CSV/sales_90.csv"

# Optional: Print the paths for verification
echo "Using the following file paths:"
echo "Customers: $CUSTOMERS_FILE_PATH"
echo "Invoices: $INVOICES_FILE_PATH"
echo "Sales Receipts: $SALES_RECEIPTS_FILE_PATH"

# Run the Meltano pipeline
meltano run --full-refresh tap-csv target-postgres dbt-postgres:run


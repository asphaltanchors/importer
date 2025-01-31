# Import Scripts

This directory contains scripts for importing data from various sources.

## Daily Import Process

The `process-daily-imports.ts` script handles automated daily imports of customer, invoice, and sales receipt data. It processes CSV files that are generated daily and maintains a history of imports.

### Directory Structure

By default, the script uses the following structure:
- `/CSV`: Base directory for new CSV files
- `/CSV/archive`: Successfully processed files are moved here
- `/CSV/failed`: Files that failed to process are moved here
- `/CSV/logs`: Import logs are stored here

You can specify a different base directory when running the script:
```bash
pnpx tsx dist/scripts/process-daily-imports.js /path/to/csv/directory
```

The script will automatically create the archive, failed, and logs subdirectories if they don't exist.

### File Naming Convention

Files should follow these naming patterns:
- Customers: `customers_YYYYMMDD.csv` (e.g., `customers_20250120.csv`)
- Invoices: `invoices_YYYYMMDD.csv` (e.g., `invoices_20250120.csv`)
- Sales Receipts: `sales_receipts_YYYYMMDD.csv` (e.g., `sales_receipts_20250120.csv`)

### Running the Import

```bash
# Run with default directory (/CSV)
pnpx tsx dist/scripts/process-daily-imports.js

# Run with custom directory
pnpx tsx dist/scripts/process-daily-imports.js /path/to/csv/directory

# Setup cron job (runs at 2 AM daily)
0 2 * * * cd /path/to/project && /usr/local/bin/pnpx tsx dist/scripts/process-daily-imports.js
```

### Import Process

1. Validates directory structure and checks for matching files
2. Processes files modified in the last 24 hours
3. For each file:
   - Validates CSV format
   - Imports data using appropriate processor
   - Moves file to archive or failed directory
4. Generates import log with results
5. Cleans up old files based on retention policy (30 days)

### Logging

Import logs are stored in JSON format with the following information:
- Date of import
- Files processed
- Success/failure status
- Error messages
- Import statistics

### Error Handling

- Failed imports don't stop the entire process
- Files that fail to import are moved to the failed directory
- Detailed error logs are generated
- Process exits with code 1 if any imports fail
- Clear error messages if:
  - Import directory doesn't exist
  - No matching CSV files are found

### Individual Import Scripts

The following scripts can also be run individually:

- `import-customer.ts`: Import customer data
- `import-invoice.ts`: Import invoice data
- `import-salesreceipt.ts`: Import sales receipt data

Each script accepts these options:
- `-d, --debug`: Enable debug logging
- `-s, --skip-lines <number>`: Skip first N lines of CSV file

Example:
```bash
pnpx tsx --expose-gc scripts/import-invoice.ts invoices.csv
```

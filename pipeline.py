# pipeline.py

import os
import glob
import csv
import json
import subprocess
from datetime import datetime

import dlt
from dotenv import load_dotenv
from domain_consolidation import analyze_domains, create_domain_mapping_table, create_customer_name_mapping_table

# 0) Load environment
load_dotenv()

# Validate DROPBOX_PATH environment variable
try:
    DROPBOX_PATH = os.environ["DROPBOX_PATH"]
except KeyError:
    print("ERROR: DROPBOX_PATH environment variable is not set.")
    print("Please set DROPBOX_PATH in your .env file or environment variables.")
    print("Example: DROPBOX_PATH=/path/to/dropbox/quickbooks-csv/input")
    exit(1)

# Validate DROPBOX_PATH directory exists
if not os.path.exists(DROPBOX_PATH):
    print(f"ERROR: DROPBOX_PATH directory does not exist: {DROPBOX_PATH}")
    print("Please check that:")
    print("1. The path is correct in your .env file")
    print("2. The directory exists and is accessible")
    print("3. You have read permissions for the directory")
    exit(1)

if not os.path.isdir(DROPBOX_PATH):
    print(f"ERROR: DROPBOX_PATH is not a directory: {DROPBOX_PATH}")
    print("Please ensure DROPBOX_PATH points to a directory, not a file.")
    exit(1)

# Check for CSV files in the directory
csv_patterns = [
    "Customer_*.csv", "01_BACKUP_Customer_*.csv",
    "Item_*.csv", "01_BACKUP_Item_*.csv", 
    "Sales*.csv", "01_BACKUP_Sales_*.csv",
    "Invoice_*.csv", "01_BACKUP_Invoice_*.csv"
]

found_files = []
for pattern in csv_patterns:
    found_files.extend(glob.glob(os.path.join(DROPBOX_PATH, pattern)))

if not found_files:
    print(f"WARNING: No CSV files found in DROPBOX_PATH: {DROPBOX_PATH}")
    print("Expected file patterns:")
    for pattern in csv_patterns:
        print(f"  - {pattern}")
    print("\nPlease ensure QuickBooks CSV exports are placed in this directory.")
    print("The pipeline will continue but may not process any data.")

print(f"Using DROPBOX_PATH: {DROPBOX_PATH}")
print(f"Found {len(found_files)} CSV files to process")

# 1) Define your source
@dlt.source
def qb_source():
    # customers resource
    @dlt.resource(
        write_disposition="merge",
        name="customers",
        primary_key=["QuickBooks Internal Id"]
    )
    def extract_customers():
        # First process backup files
        backup_pattern = os.path.join(DROPBOX_PATH, "01_BACKUP_Customer_*.csv")
        for f in sorted(glob.glob(backup_pattern)):
            print(f"Processing backup file: {f}")
            with open(f, newline="") as fh:
                rdr = csv.DictReader(fh)
                for row in rdr:
                    yield {
                        **row,
                        "load_date": datetime.utcnow().date().isoformat(),
                        "is_backup": True
                    }
        
        # Then process daily files
        pattern = os.path.join(DROPBOX_PATH, "Customer_*.csv")
        for f in sorted(glob.glob(pattern)):
            print(f"Processing daily file: {f}")
            with open(f, newline="") as fh:
                rdr = csv.DictReader(fh)
                for row in rdr:
                    yield {
                        **row,
                        "load_date": datetime.utcnow().date().isoformat(),
                        "is_backup": False
                    }

    # items resource
    @dlt.resource(
        write_disposition="merge",
        name="items",
        primary_key=["Item Name", "snapshot_date"]  # Composite key with item name and date
    )
    def extract_items():
        import re
        
        # Group files by date to ensure we process the latest file for each day
        files_by_date = {}
        
        # First process backup files - they take precedence for their dates
        backup_pattern = os.path.join(DROPBOX_PATH, "01_BACKUP_Item_*.csv")
        for f in glob.glob(backup_pattern):
            # Extract date from backup filename (format: 01_BACKUP_Item_MM_DD_YYYY.csv)
            filename = os.path.basename(f)
            date_match = re.match(r'01_BACKUP_Item_(\d{2})_(\d{2})_(\d{4})\.csv', filename)
            
            if date_match:
                month, day, year = date_match.groups()
                
                # Format as ISO date (YYYY-MM-DD)
                date_str = f"{year}-{month}-{day}"
                
                # Create a datetime object for sorting - use a high time to ensure precedence
                file_datetime = datetime(
                    int(year), int(month), int(day), 23, 59, 59
                )
                
                # Always use backup files if available
                files_by_date[date_str] = {
                    'path': f,
                    'datetime': file_datetime,
                    'is_backup': True
                }
                print(f"Processing backup item file for date {date_str}: {f}")
        
        # Then process daily files
        daily_pattern = os.path.join(DROPBOX_PATH, "Item_*.csv")
        for f in glob.glob(daily_pattern):
            # Extract date from filename (format: Item_MM_DD_YYYY_H_MM_SS.csv)
            filename = os.path.basename(f)
            date_match = re.match(r'Item_(\d{2})_(\d{2})_(\d{4})_(\d+)_(\d{2})_(\d{2})\.csv', filename)
            
            if date_match:
                month, day, year, hour, minute, second = date_match.groups()
                
                # Format as ISO date (YYYY-MM-DD)
                date_str = f"{year}-{month}-{day}"
                
                # Create a datetime object for sorting
                file_datetime = datetime(
                    int(year), int(month), int(day), 
                    int(hour), int(minute), int(second)
                )
                
                # Group by date, keeping track of the full path and datetime
                # Only use daily file if we don't have a backup for this date
                # or if this daily file is newer than the previous daily file
                if date_str not in files_by_date:
                    files_by_date[date_str] = {
                        'path': f,
                        'datetime': file_datetime,
                        'is_backup': False
                    }
                elif not files_by_date[date_str].get('is_backup', False) and file_datetime > files_by_date[date_str]['datetime']:
                    files_by_date[date_str] = {
                        'path': f,
                        'datetime': file_datetime,
                        'is_backup': False
                    }
        
        # Process the files in date order
        for date_str, file_info in sorted(files_by_date.items()):
            print(f"Processing item file for date {date_str}: {file_info['path']}")
            with open(file_info['path'], newline="") as fh:
                rdr = csv.DictReader(fh)
                for row in rdr:
                    yield {
                        **row,
                        "load_date": datetime.utcnow().date().isoformat(),
                        "snapshot_date": date_str,  # Add the extracted date
                        "is_backup": file_info.get('is_backup', False)
                    }

    # sales_receipts
    @dlt.resource(
        write_disposition="merge",
        name="sales_receipts",
        primary_key=["QuickBooks Internal Id", "Product/Service"]
    )
    def extract_sales_receipts():
        # First process backup files
        backup_pattern = os.path.join(DROPBOX_PATH, "01_BACKUP_Sales_*.csv")
        for f in sorted(glob.glob(backup_pattern)):
            print(f"Processing backup sales file: {f}")
            with open(f, newline="") as fh:
                rdr = csv.DictReader(fh)
                for row in rdr:
                    yield { 
                        **row, 
                        "load_date": datetime.utcnow().date().isoformat(),
                        "is_backup": True
                    }
                    
        # Then process daily files
        pattern = os.path.join(DROPBOX_PATH, "Sales*.csv")
        for f in sorted(glob.glob(pattern)):
            print(f"Processing daily sales file: {f}")
            with open(f, newline="") as fh:
                rdr = csv.DictReader(fh)
                for row in rdr:
                    yield { 
                        **row, 
                        "load_date": datetime.utcnow().date().isoformat(),
                        "is_backup": False
                    }

    # invoices
    @dlt.resource(
        write_disposition="merge",
        name="invoices",
        primary_key=["QuickBooks Internal Id", "Product/Service"]
    )
    def extract_invoices():
        # First process backup files
        backup_pattern = os.path.join(DROPBOX_PATH, "01_BACKUP_Invoice_*.csv")
        for f in sorted(glob.glob(backup_pattern)):
            print(f"Processing backup invoice file: {f}")
            with open(f, newline="") as fh:
                rdr = csv.DictReader(fh)
                for row in rdr:
                    yield { 
                        **row, 
                        "load_date": datetime.utcnow().date().isoformat(),
                        "is_backup": True
                    }
                    
        # Then process daily files
        pattern = os.path.join(DROPBOX_PATH, "Invoice_*.csv")
        for f in sorted(glob.glob(pattern)):
            print(f"Processing daily invoice file: {f}")
            with open(f, newline="") as fh:
                rdr = csv.DictReader(fh)
                for row in rdr:
                    yield { 
                        **row, 
                        "load_date": datetime.utcnow().date().isoformat(),
                        "is_backup": False
                    }

    # company_enrichment resource
    @dlt.resource(
        write_disposition="merge",
        name="company_enrichment",
        primary_key=["company_domain"]
    )
    def extract_company_enrichment():
        """One-time load of pre-enriched company data from JSONL file"""
        enrichment_file = os.path.join(DROPBOX_PATH, "company_enrichment.jsonl")
        print(f"Checking for company enrichment file at: {enrichment_file}")
        
        if os.path.exists(enrichment_file):
            file_size = os.path.getsize(enrichment_file)
            print(f"Found company enrichment file: {enrichment_file} ({file_size} bytes)")
            
            record_count = 0
            with open(enrichment_file, 'r') as fh:
                for line_num, line in enumerate(fh, 1):
                    line = line.strip()
                    if line:  # Skip empty lines
                        try:
                            data = json.loads(line)
                            record_count += 1
                            yield {
                                **data,
                                "load_date": datetime.utcnow().date().isoformat(),
                                "is_manual_load": True
                            }
                        except json.JSONDecodeError as e:
                            print(f"Warning: Failed to parse JSON line {line_num}: {e}")
                            print(f"Problematic line: {line[:100]}...")
                            continue
            
            print(f"Company enrichment: processed {record_count} records from {enrichment_file}")
        else:
            print(f"Company enrichment file not found: {enrichment_file}")
            print(f"Directory contents: {os.listdir(DROPBOX_PATH) if os.path.exists(DROPBOX_PATH) else 'DROPBOX_PATH does not exist'}")

    # **Return** your resource functions in a list
    return [
        extract_customers,
        extract_items,
        extract_sales_receipts,
        extract_invoices,
        extract_company_enrichment
    ]

# 3) Run it
if __name__ == "__main__":
    # 1. Run DLT pipeline to load data
    load_pipeline = dlt.pipeline(
        pipeline_name="dqi_pipeline",
        destination="postgres",
        dataset_name="raw",
    )
    load_info = load_pipeline.run(qb_source())
    print("DLT pipeline complete:", load_info)
    
    # 2. Run domain consolidation to create mapping tables
    print("\nRunning domain consolidation...")
    try:
        domain_stats, normalization_mapping = analyze_domains()
        create_domain_mapping_table()
        create_customer_name_mapping_table()
        print("Domain consolidation complete: raw.domain_mapping and raw.customer_name_mapping tables created")
    except Exception as e:
        print(f"❌ Error during domain consolidation: {e}")
        raise
    
    # 3. Run DBT transformations
    print("\nRunning DBT transformations...")
    try:
        result = subprocess.run(["dbt", "run"], check=True, capture_output=True, text=True)
        print("DBT transformations complete:", result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"❌ DBT run failed: {e}")
        print("STDOUT:", e.stdout)
        print("STDERR:", e.stderr)
        raise
    
    print("\n✅ Complete pipeline finished successfully!")
    print("Data flow: DLT extraction → Domain consolidation → DBT transformations")
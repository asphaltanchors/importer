# pipeline.py

import os
import glob
import csv
from datetime import datetime

import dlt
from dotenv import load_dotenv
from matcher import normalize_company_name

# 0) Load environment
load_dotenv()
DROPBOX_PATH = os.environ["DROPBOX_PATH"]

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
                        "canonical_name": normalize_company_name(row.get("Company Name", "")),
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
                        "canonical_name": normalize_company_name(row.get("Company Name", "")),
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

    # **Return** your resource functions in a list
    return [
        extract_customers,
        extract_items,
        extract_sales_receipts,
        extract_invoices
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
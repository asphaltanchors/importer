# ABOUTME: Main DLT pipeline for QuickBooks XLSX data extraction and transformation
# ABOUTME: Processes QuickBooks XLSX exports and loads all worksheets into PostgreSQL raw schema

import os
import glob
import json
import subprocess
import re
from datetime import datetime
from pathlib import Path

import dlt
import pandas as pd
from dotenv import load_dotenv
from domain_consolidation import analyze_domains, create_domain_mapping_table, create_customer_name_mapping_table

# Load environment
load_dotenv()

# Validate DROPBOX_PATH environment variable
try:
    DROPBOX_PATH = os.environ["DROPBOX_PATH"]
except KeyError:
    print("ERROR: DROPBOX_PATH environment variable is not set.")
    print("Please set DROPBOX_PATH in your .env file or environment variables.")
    print("Example: DROPBOX_PATH=/path/to/dropbox/quickbooks-xlsx/input")
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

# Check for XLSX files in the directory
xlsx_patterns = [
    "all-list*.xlsx", "01_BACKUP_all-list*.xlsx",
    "all-transaction*.xlsx", "01_BACKUP_all-transaction*.xlsx"
]

found_files = []
for pattern in xlsx_patterns:
    found_files.extend(glob.glob(os.path.join(DROPBOX_PATH, pattern)))

if not found_files:
    print(f"WARNING: No XLSX files found in DROPBOX_PATH: {DROPBOX_PATH}")
    print("Expected file patterns:")
    for pattern in xlsx_patterns:
        print(f"  - {pattern}")
    print("\nPlease ensure QuickBooks XLSX exports are placed in this directory.")
    print("The pipeline will continue but may not process any data.")

print(f"Using DROPBOX_PATH: {DROPBOX_PATH}")
print(f"Found {len(found_files)} XLSX files to process")

# Define worksheet mappings
LIST_WORKSHEETS = [
    'Account', 'Vendor', 'Employee', 'Item Discount', 'Item', 'Customer'
]

TRANSACTION_WORKSHEETS = [
    'Invoice', 'Sales Order', 'Estimate', 'Sales Receipt', 'Credit Memo', 
    'Payment', 'Purchase Order', 'Bill', 'Bill Payment', 'Check', 
    'Credit Card Charge', 'Trial Balance', 'Deposit', 'Inventory Adjustment', 
    'Journal Entry', 'Build Assembly', 'Custom Txn Detail'
]

def extract_date_from_filename(filename):
    """Extract date from XLSX filename"""
    # Handle backup files: 01_BACKUP_all-list_MM_DD_YYYY.xlsx
    backup_match = re.match(r'01_BACKUP_all-(?:list|transaction)_(\d{2})_(\d{2})_(\d{4})\.xlsx', filename)
    if backup_match:
        month, day, year = backup_match.groups()
        return f"{year}-{month}-{day}", True
    
    # Handle daily files: all-list_MM_DD_YYYY_H_MM_SS.xlsx
    daily_match = re.match(r'all-(?:list|transaction)_(\d{2})_(\d{2})_(\d{4})_(\d+)_(\d{2})_(\d{2})\.xlsx', filename)
    if daily_match:
        month, day, year, hour, minute, second = daily_match.groups()
        return f"{year}-{month}-{day}", False
    
    # Fallback for test files
    if 'test' in filename.lower():
        return datetime.now().strftime('%Y-%m-%d'), False
        
    raise ValueError(f"Could not extract date from filename: {filename}")

def get_file_priority(files):
    """Sort files by date and type, prioritizing backup files"""
    files_by_date = {}
    
    for filepath in files:
        filename = os.path.basename(filepath)
        try:
            date_str, is_backup = extract_date_from_filename(filename)
            file_datetime = datetime.strptime(date_str, '%Y-%m-%d')
            
            key = (date_str, 'backup' if is_backup else 'daily')
            if key not in files_by_date or is_backup:  # Backup files take precedence
                files_by_date[key] = {
                    'path': filepath,
                    'date': date_str,
                    'is_backup': is_backup,
                    'datetime': file_datetime
                }
        except ValueError as e:
            print(f"Warning: {e}")
            continue
    
    # Return sorted by date, backup files first for each date
    return sorted(files_by_date.values(), key=lambda x: (x['datetime'], not x['is_backup']))

def standardize_column_names(df):
    """Standardize column names for consistency"""
    # Remove extra spaces, convert to snake_case equivalent
    df.columns = [col.strip().replace('/', '_').replace(' ', '_').replace('.', '') for col in df.columns]
    return df

def process_worksheet_data(df, worksheet_name, file_info):
    """Process and enrich worksheet data"""
    df = standardize_column_names(df)
    
    # Add metadata columns
    for _, row in df.iterrows():
        yield {
            **row.to_dict(),
            "load_date": datetime.utcnow().date().isoformat(),
            "snapshot_date": file_info['date'],
            "is_backup": file_info['is_backup'],
            "worksheet_name": worksheet_name,
            "source_file": os.path.basename(file_info['path'])
        }

@dlt.source
def xlsx_quickbooks_source():
    """Extract all QuickBooks XLSX worksheets"""
    
    # Process list files
    list_files = []
    for pattern in ["all-list*.xlsx", "01_BACKUP_all-list*.xlsx"]:
        list_files.extend(glob.glob(os.path.join(DROPBOX_PATH, pattern)))
    
    list_files_sorted = get_file_priority(list_files)
    
    # Process transaction files  
    transaction_files = []
    for pattern in ["all-transaction*.xlsx", "01_BACKUP_all-transaction*.xlsx"]:
        transaction_files.extend(glob.glob(os.path.join(DROPBOX_PATH, pattern)))
    
    transaction_files_sorted = get_file_priority(transaction_files)
    
    # Create resources for each worksheet type
    resources = []
    
    # List worksheets - create each resource individually to fix closure issues
    def create_list_resource(worksheet_name):
        table_name = f"xlsx_{worksheet_name.lower().replace(' ', '_')}"
        
        @dlt.resource(
            write_disposition="merge",
            name=table_name,
            primary_key=["QuickBooks_Internal_Id", "snapshot_date"]
        )
        def extract_list_worksheet():
            for file_info in list_files_sorted:
                try:
                    print(f"Processing {worksheet_name} from {file_info['path']}")
                    df = pd.read_excel(file_info['path'], sheet_name=worksheet_name)
                    if len(df) > 0:
                        yield from process_worksheet_data(df, worksheet_name, file_info)
                    else:
                        print(f"No data in {worksheet_name} worksheet")
                except Exception as e:
                    print(f"Error processing {worksheet_name} from {file_info['path']}: {e}")
        
        return extract_list_worksheet
    
    for worksheet_name in LIST_WORKSHEETS:
        resources.append(create_list_resource(worksheet_name))
    
    # Transaction worksheets - create each resource individually to fix closure issues
    def create_transaction_resource(worksheet_name):
        table_name = f"xlsx_{worksheet_name.lower().replace(' ', '_')}"
        
        # Set primary key based on worksheet structure
        if worksheet_name == 'Trial Balance':
            primary_key = ["S_No", "Trial_Balance_No", "Account_Name", "snapshot_date"]
        elif worksheet_name in ['Custom Txn Detail']:
            primary_key = ["S_No", "snapshot_date"]  # These may not have QuickBooks_Internal_Id
        else:
            primary_key = ["QuickBooks_Internal_Id", "S_No"] 
        
        @dlt.resource(
            write_disposition="merge", 
            name=table_name,
            primary_key=primary_key
        )
        def extract_transaction_worksheet():
            for file_info in transaction_files_sorted:
                try:
                    print(f"Processing {worksheet_name} from {file_info['path']}")
                    df = pd.read_excel(file_info['path'], sheet_name=worksheet_name)
                    if len(df) > 0:
                        yield from process_worksheet_data(df, worksheet_name, file_info)
                    else:
                        print(f"No data in {worksheet_name} worksheet")
                except Exception as e:
                    print(f"Error processing {worksheet_name} from {file_info['path']}: {e}")
        
        return extract_transaction_worksheet
    
    for worksheet_name in TRANSACTION_WORKSHEETS:
        resources.append(create_transaction_resource(worksheet_name))
    
    # Company enrichment resource (keep existing)
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
    
    resources.append(extract_company_enrichment)
    
    return resources

# Run the pipeline
if __name__ == "__main__":
    # 1. Run DLT pipeline to load XLSX data
    load_pipeline = dlt.pipeline(
        pipeline_name="xlsx_quickbooks_pipeline",
        destination="postgres", 
        dataset_name="raw",
    )
    load_info = load_pipeline.run(xlsx_quickbooks_source())
    print("DLT XLSX pipeline complete:", load_info)
    
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
    
    print("\n✅ Complete XLSX pipeline finished successfully!")
    print("Data flow: XLSX extraction → Domain consolidation → DBT transformations")
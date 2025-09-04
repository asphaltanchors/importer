# ABOUTME: Main DLT pipeline for QuickBooks XLSX data extraction and transformation
# ABOUTME: Supports seed (full historical) and incremental (daily) loading modes

import os
import sys
import glob
import json
import subprocess
import re
import argparse
from datetime import datetime, UTC
from pathlib import Path

import dlt
import pandas as pd

# Add pipelines directory to path for shared imports  
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from shared import get_database_url, get_dlt_destination, validate_environment_variables
from domain_consolidation import analyze_domains, create_domain_mapping_table, create_customer_name_mapping_table

# Validate required environment variables
REQUIRED_ENV_VARS = ["DROPBOX_PATH", "DATABASE_URL"]

try:
    env_vars = validate_environment_variables(REQUIRED_ENV_VARS)
    DROPBOX_PATH = env_vars["DROPBOX_PATH"]
except ValueError as e:
    print(f"ERROR: {e}")
    print("Please set required variables in your .env file or environment variables.")
    print("Required: DROPBOX_PATH, DATABASE_URL")
    exit(1)

# Validate DROPBOX_PATH directory exists
if not os.path.exists(DROPBOX_PATH):
    print(f"ERROR: DROPBOX_PATH directory does not exist: {DROPBOX_PATH}")
    exit(1)

if not os.path.isdir(DROPBOX_PATH):
    print(f"ERROR: DROPBOX_PATH is not a directory: {DROPBOX_PATH}")
    exit(1)

# Define paths
SEED_PATH = os.path.join(DROPBOX_PATH, "seed")
INPUT_PATH = os.path.join(DROPBOX_PATH, "input")

print(f"Using DROPBOX_PATH: {DROPBOX_PATH}")
print(f"Seed directory: {SEED_PATH}")
print(f"Input directory: {INPUT_PATH}")

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
    """Extract date from daily XLSX filename"""
    # Pattern: {DATE}_transactions.xlsx or {DATE}_lists.xlsx
    match = re.match(r'(\d{4}-\d{2}-\d{2})_(?:transactions|lists)\.xlsx', filename)
    if match:
        return match.group(1)
    
    # Pattern: All Lists_MM_DD_YYYY_HH_MM_SS.xlsx or All Transactions_MM_DD_YYYY_HH_MM_SS.xlsx
    match = re.match(r'All (?:Lists|Transactions)_(\d{2})_(\d{2})_(\d{4})_\d{1,2}_\d{1,2}_\d{1,2}\.xlsx?', filename)
    if match:
        mm, dd, yyyy = match.groups()
        return f"{yyyy}-{mm}-{dd}"
    
    # Fallback for test files
    if 'test' in filename.lower():
        return datetime.now().strftime('%Y-%m-%d')
        
    raise ValueError(f"Could not extract date from filename: {filename}")

def get_daily_files(input_path, file_type=None, latest_only=False):
    """Get daily files from input directory
    
    Args:
        input_path: Path to input directory
        file_type: 'transactions' or 'lists' to filter, None for both
        latest_only: If True, return only the most recent file for each type
    """
    if file_type:
        # Support both old and new naming patterns
        patterns = [
            f"*_{file_type}.xlsx",  # Old pattern: YYYY-MM-DD_transactions.xlsx
            f"All {file_type.title()}*.xlsx",  # New pattern: All Lists_MM_DD_YYYY_HH_MM_SS.xlsx
            f"All {file_type.title()}*.xls"   # New pattern with .xls extension
        ]
    else:
        patterns = [
            "*_*.xlsx",  # Old pattern
            "All Lists*.xlsx",  # New pattern for lists
            "All Lists*.xls",
            "All Transactions*.xlsx",  # New pattern for transactions
            "All Transactions*.xls"
        ]
    
    files = []
    for pattern in patterns:
        files.extend(glob.glob(os.path.join(input_path, pattern)))
    
    if not files:
        return []
    
    # Parse and sort files by date
    file_info = []
    for filepath in files:
        filename = os.path.basename(filepath)
        try:
            date_str = extract_date_from_filename(filename)
            # Determine file type from filename
            if 'transactions' in filename.lower():
                file_type_detected = 'transactions'
            elif 'lists' in filename.lower():
                file_type_detected = 'lists'
            else:
                file_type_detected = 'unknown'
            
            file_info.append({
                'path': filepath,
                'filename': filename,
                'date': date_str,
                'datetime': datetime.strptime(date_str, '%Y-%m-%d'),
                'type': file_type_detected
            })
        except ValueError as e:
            print(f"Warning: {e}")
            continue
    
    # Sort by date (newest first)
    file_info.sort(key=lambda x: x['datetime'], reverse=True)
    
    if latest_only:
        # Return only the latest file for each type
        latest_files = {}
        for info in file_info:
            if info['type'] not in latest_files:
                latest_files[info['type']] = info
        return list(latest_files.values())
    
    return file_info

def standardize_column_names(df):
    """Standardize column names for consistency"""
    df.columns = [col.strip().replace('/', '_').replace(' ', '_').replace('.', '') for col in df.columns]
    return df

# Global cache for XLSX files to avoid re-reading
_xlsx_file_cache = {}

def get_xlsx_worksheets(file_path):
    """Read XLSX file once and cache all worksheets"""
    if file_path not in _xlsx_file_cache:
        print(f"Loading XLSX file into cache: {os.path.basename(file_path)}")
        try:
            # Read all sheets at once
            all_sheets = pd.read_excel(file_path, sheet_name=None)
            _xlsx_file_cache[file_path] = all_sheets
            print(f"Cached {len(all_sheets)} worksheets from {os.path.basename(file_path)}")
        except Exception as e:
            print(f"Error caching XLSX file {file_path}: {e}")
            _xlsx_file_cache[file_path] = {}
    
    return _xlsx_file_cache[file_path]

def replace_nulls_recursive(obj):
    """Recursively replace None/null values with empty strings in nested objects"""
    if isinstance(obj, dict):
        return {k: replace_nulls_recursive(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_nulls_recursive(item) for item in obj]
    elif obj is None:
        return ''
    else:
        return obj

def process_worksheet_data(df, worksheet_name, file_info, is_seed=False, chunk_size=1000):
    """Process and enrich worksheet data efficiently using vectorized operations"""
    if df.empty:
        return
    
    df = standardize_column_names(df)
    
    # Add metadata columns using vectorized operations
    df = df.copy()
    df["load_date"] = datetime.now(UTC).date().isoformat()
    df["snapshot_date"] = file_info.get('date', datetime.now().strftime('%Y-%m-%d'))
    df["is_seed"] = is_seed
    df["worksheet_name"] = worksheet_name
    df["source_file"] = os.path.basename(file_info['path'])
    
    # Convert to records in chunks for memory efficiency
    total_rows = len(df)
    
    # Debug: Log column info for first chunk
    if total_rows > 0:
        print(f"  Worksheet {worksheet_name}: {total_rows} rows, {len(df.columns)} columns")
        # Check for columns that are all NaN/None
        null_cols = df.columns[df.isnull().all()].tolist()
        if null_cols:
            print(f"  WARNING: {len(null_cols)} completely null columns: {null_cols[:5]}...")
    
    for i in range(0, total_rows, chunk_size):
        chunk = df.iloc[i:i+chunk_size]
        # Replace NaN with empty string to preserve columns for DLT
        chunk = chunk.fillna('')
        records = chunk.to_dict('records')
        yield from records

@dlt.source
def xlsx_quickbooks_source(mode="full"):
    """Extract QuickBooks XLSX worksheets
    
    Args:
        mode: 'seed', 'incremental', or 'full'
              - 'seed': Load only seed data (historical)
              - 'incremental': Load only latest daily data
              - 'full': Load seed + all incremental data
    """
    resources = []
    
    # Determine which files to process
    files_to_process = []
    
    if mode in ['seed', 'full']:
        # Add seed files
        seed_lists = os.path.join(SEED_PATH, "all_lists.xlsx")
        seed_transactions = os.path.join(SEED_PATH, "all_transactions.xlsx")
        
        if os.path.exists(seed_lists):
            files_to_process.append({
                'path': seed_lists,
                'filename': 'all_lists.xlsx',
                'date': 'seed',
                'type': 'lists',
                'is_seed': True
            })
            print(f"Found seed lists file: {seed_lists}")
        
        if os.path.exists(seed_transactions):
            files_to_process.append({
                'path': seed_transactions,
                'filename': 'all_transactions.xlsx',
                'date': 'seed',
                'type': 'transactions',
                'is_seed': True
            })
            print(f"Found seed transactions file: {seed_transactions}")
    
    if mode in ['incremental', 'full']:
        # Add incremental files
        if mode == 'incremental':
            # Only latest files for cron jobs
            daily_files = get_daily_files(INPUT_PATH, latest_only=True)
        else:
            # All historical daily files for full bootstrap
            daily_files = get_daily_files(INPUT_PATH, latest_only=False)
        
        for file_info in daily_files:
            file_info['is_seed'] = False
            files_to_process.append(file_info)
        
        print(f"Found {len(daily_files)} daily files in {INPUT_PATH}")
    
    print(f"Processing {len(files_to_process)} files in {mode} mode")
    
    # Group files by type for processing
    list_files = [f for f in files_to_process if f['type'] == 'lists']
    transaction_files = [f for f in files_to_process if f['type'] == 'transactions']
    
    # Create resources for list worksheets
    def create_list_resource(worksheet_name):
        table_name = f"xlsx_{worksheet_name.lower().replace(' ', '_')}"
        
        # Define column type hints for numeric fields
        columns = {}
        if worksheet_name == 'Item':
            columns = {
                "sales_price": {"data_type": "decimal"},
                "purchase_cost": {"data_type": "decimal"}, 
                "quantity_on_hand": {"data_type": "decimal"},
                "quantity_on_order": {"data_type": "decimal"},
                "quantity_on_sales_order": {"data_type": "decimal"},
                "min_re_order_point": {"data_type": "decimal"},
                "max_re_order_point": {"data_type": "decimal"}
            }
        elif worksheet_name == 'Customer':
            columns = {
                "current_balance": {"data_type": "decimal"},
                "credit_limit": {"data_type": "decimal"}
            }
        
        @dlt.resource(
            write_disposition="merge",
            name=table_name,
            primary_key=["QuickBooks_Internal_Id"],
            columns=columns
        )
        def extract_list_worksheet():
            for file_info in list_files:
                try:
                    print(f"Processing {worksheet_name} from {file_info['filename']}")
                    
                    # Use cached worksheets instead of re-reading file
                    all_sheets = get_xlsx_worksheets(file_info['path'])
                    
                    if worksheet_name in all_sheets:
                        df = all_sheets[worksheet_name]
                        if len(df) > 0:
                            yield from process_worksheet_data(df, worksheet_name, file_info, file_info['is_seed'])
                        else:
                            print(f"No data in {worksheet_name} worksheet")
                    else:
                        print(f"Worksheet {worksheet_name} not found in {file_info['filename']}")
                except Exception as e:
                    print(f"Error processing {worksheet_name} from {file_info['filename']}: {e}")
        
        return extract_list_worksheet
    
    for worksheet_name in LIST_WORKSHEETS:
        resources.append(create_list_resource(worksheet_name))
    
    # Create resources for transaction worksheets
    def create_transaction_resource(worksheet_name):
        table_name = f"xlsx_{worksheet_name.lower().replace(' ', '_')}"
        
        # Set primary key based on worksheet structure
        if worksheet_name == 'Trial Balance':
            primary_key = ["S_No", "Trial_Balance_No", "Account_Name", "snapshot_date"]
        elif worksheet_name in ['Custom Txn Detail']:
            primary_key = ["S_No", "snapshot_date"]
        else:
            primary_key = ["QuickBooks_Internal_Id", "S_No"] 
        
        @dlt.resource(
            write_disposition="merge", 
            name=table_name,
            primary_key=primary_key
        )
        def extract_transaction_worksheet():
            for file_info in transaction_files:
                try:
                    print(f"Processing {worksheet_name} from {file_info['filename']}")
                    
                    # Use cached worksheets instead of re-reading file
                    all_sheets = get_xlsx_worksheets(file_info['path'])
                    
                    if worksheet_name in all_sheets:
                        df = all_sheets[worksheet_name]
                        if len(df) > 0:
                            yield from process_worksheet_data(df, worksheet_name, file_info, file_info['is_seed'])
                        else:
                            print(f"No data in {worksheet_name} worksheet")
                    else:
                        print(f"Worksheet {worksheet_name} not found in {file_info['filename']}")
                except Exception as e:
                    print(f"Error processing {worksheet_name} from {file_info['filename']}: {e}")
        
        return extract_transaction_worksheet
    
    for worksheet_name in TRANSACTION_WORKSHEETS:
        resources.append(create_transaction_resource(worksheet_name))
    
    # Company enrichment resource (only load in seed or full mode)
    if mode in ['seed', 'full']:
        @dlt.resource(
            write_disposition="merge",
            name="company_enrichment", 
            primary_key=["company_domain"]
        )
        def extract_company_enrichment():
            """Load pre-enriched company data from JSONL file"""
            enrichment_file = os.path.join(SEED_PATH, "company_enrichment.jsonl")
            print(f"Checking for company enrichment file at: {enrichment_file}")
            
            if os.path.exists(enrichment_file):
                file_size = os.path.getsize(enrichment_file)
                print(f"Found company enrichment file: {enrichment_file} ({file_size} bytes)")
                
                record_count = 0
                with open(enrichment_file, 'r') as fh:
                    for line_num, line in enumerate(fh, 1):
                        line = line.strip()
                        if line:
                            try:
                                data = json.loads(line)
                                # Replace null values with empty strings to help DLT type inference
                                data = replace_nulls_recursive(data)
                                record_count += 1
                                yield {
                                    **data,
                                    "load_date": datetime.now(UTC).date().isoformat(),
                                    "is_seed": True
                                }
                            except json.JSONDecodeError as e:
                                print(f"Warning: Failed to parse JSON line {line_num}: {e}")
                                continue
                
                print(f"Company enrichment: processed {record_count} records")
            else:
                print(f"Company enrichment file not found: {enrichment_file}")
        
        resources.append(extract_company_enrichment)
        
    
    
    return resources

def run_dbt_transformations():
    """Run DBT transformations"""
    print("\nRunning DBT transformations...")
    try:
        # Change to project root directory for DBT commands
        original_cwd = os.getcwd()
        project_root = os.path.join(os.path.dirname(__file__), "../..")
        os.chdir(project_root)
        
        result = subprocess.run(["dbt", "run"], check=True, capture_output=True, text=True)
        print("DBT transformations complete")
        
        # Return to original directory
        os.chdir(original_cwd)
        return True
    except subprocess.CalledProcessError as e:
        # Return to original directory on error
        os.chdir(original_cwd)
        print(f"❌ DBT run failed: {e}")
        print("STDOUT:", e.stdout)
        print("STDERR:", e.stderr)
        return False

def run_domain_consolidation():
    """Run domain consolidation to create mapping tables"""
    print("\nRunning domain consolidation...")
    try:
        domain_stats, normalization_mapping = analyze_domains()
        create_domain_mapping_table()
        create_customer_name_mapping_table()
        print("Domain consolidation complete")
        return True
    except Exception as e:
        print(f"❌ Error during domain consolidation: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='QuickBooks XLSX Pipeline')
    parser.add_argument('--mode', 
                       choices=['seed', 'incremental', 'full'], 
                       default='full',
                       help='Loading mode: seed (historical only), incremental (latest daily), full (seed + all incremental)')
    
    args = parser.parse_args()
    
    print(f"Running QuickBooks XLSX pipeline in {args.mode} mode")
    
    # 1. Run DLT pipeline to load XLSX data
    load_pipeline = dlt.pipeline(
        pipeline_name="xlsx_quickbooks_pipeline",
        destination=get_dlt_destination(), 
        dataset_name="raw",
    )
    
    try:
        load_info = load_pipeline.run(xlsx_quickbooks_source(mode=args.mode))
        print("DLT XLSX pipeline complete:", load_info)
    except Exception as e:
        print(f"❌ DLT pipeline failed: {e}")
        return False
    
    # 2. Run domain consolidation (only needed for full/seed loads)
    if args.mode in ['seed', 'full']:
        if not run_domain_consolidation():
            return False
    
    # 3. Run DBT transformations
    if not run_dbt_transformations():
        return False
    
    print(f"\n✅ {args.mode.title()} pipeline finished successfully!")
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
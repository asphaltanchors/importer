#!/usr/bin/env python3
"""
Pipeline script for MQI data processing.

This script replaces run_pipeline.sh and provides two main modes of operation:
1. Full import: Drops and recreates all data (similar to original script)
2. Daily import: Processes files from a directory in date sequence

For daily imports, the script uses a database table to track processed files instead of
physically moving them. This allows for easier reprocessing and provides a record of all imports.

Usage:
  Full import (default paths): ./pipeline.py --full
  Full import (custom dir): ./pipeline.py --full --dir /path/to/files/directory
  Full import test: ./pipeline.py --full --dir /path/to/files/directory --test
  Daily import: ./pipeline.py --daily --dir /path/to/parent/directory
  Daily import test: ./pipeline.py --daily --dir /path/to/parent/directory --test
  Dry run: ./pipeline.py --daily --dir /path/to/parent/directory --dry-run
"""

import os
import sys
import argparse
import subprocess
import glob
import re
from datetime import datetime
import logging
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# File type constants
FILE_TYPES = {
    'item': 'Item',
    'customer': 'Customer',
    'invoice': 'Invoice',
    'sales_receipt': 'Sales Receipt'
}

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='MQI Data Pipeline')
    
    # Create a mutually exclusive group for mode selection
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument('--full', action='store_true', 
                           help='Run full import (drops and recreates all data)')
    mode_group.add_argument('--daily', action='store_true',
                           help='Run daily import from the specified directory')
    
    # Directory option (can be used with either mode)
    parser.add_argument('--dir', metavar='DIRECTORY',
                       help='Directory containing the files to process')
    
    # Additional options
    parser.add_argument('--move-files', action='store_true',
                       help='Move files after processing (not implemented yet)')
    parser.add_argument('--archive', action='store_true',
                       help='Archive processed files (not implemented yet)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be done without executing')
    parser.add_argument('--test', action='store_true',
                       help='Test mode: scan files and show what would be processed without executing')
    
    return parser.parse_args()

def run_command(command, dry_run=False):
    """Run a shell command and log the output."""
    logger.info(f"Running command: {command}")
    
    if dry_run:
        logger.info("[DRY RUN] Command would be executed")
        return 0
    
    try:
        result = subprocess.run(command, shell=True, check=True, 
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True)
        logger.info(result.stdout)
        return 0
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with exit code {e.returncode}")
        logger.error(f"Error output: {e.stderr}")
        return e.returncode

def find_latest_full_files(directory):
    """Find the latest full dataset files in the directory."""
    # Pattern: item_all_MM_DD_YYYY.csv, customer_all_MM_DD_YYYY.csv, etc.
    latest_files = {
        "item": None,
        "customer": None,
        "invoice": None,
        "sales_receipt": None
    }
    latest_dates = {
        "item": None,
        "customer": None,
        "invoice": None,
        "sales_receipt": None
    }
    
    # Get all CSV files in the directory
    csv_files = glob.glob(os.path.join(directory, "*.csv"))
    
    for file_path in csv_files:
        filename = os.path.basename(file_path)
        
        # Check for full dataset files
        for file_type in ["item", "customer", "invoice", "sales_receipt"]:
            pattern = rf"{file_type}_all_(\d{{2}})_(\d{{2}})_(\d{{4}})\.csv"
            match = re.match(pattern, filename)
            
            if match:
                month, day, year = match.groups()
                try:
                    file_date = datetime(int(year), int(month), int(day))
                    
                    # If this is the first file of this type or it's newer than what we have
                    if latest_dates[file_type] is None or file_date > latest_dates[file_type]:
                        latest_files[file_type] = file_path
                        latest_dates[file_type] = file_date
                except ValueError:
                    logger.warning(f"Invalid date in filename: {filename}")
    
    # Check if we found all required files
    missing_types = []
    for file_type in ["item", "customer", "invoice", "sales_receipt"]:
        if latest_files[file_type] is None:
            missing_types.append(file_type)
    
    if missing_types:
        logger.error(f"Missing full dataset files: {', '.join(missing_types)}")
        return None
    
    return latest_files

def run_full_import(directory=None, dry_run=False):
    """Run the full import process (drops schemas and recreates all data)."""
    logger.info("Starting full import process")
    
    # If directory is provided, look for files there
    if directory:
        logger.info(f"Looking for full dataset files in: {directory}")
        full_files = find_latest_full_files(directory)
        
        if not full_files:
            logger.error("Could not find all required full dataset files")
            return 1
        
        # Import the full dataset
        logger.info("Importing full dataset from directory")
        os.environ["ITEMS_FILE_PATH"] = full_files["item"]
        os.environ["CUSTOMERS_FILE_PATH"] = full_files["customer"]
        os.environ["INVOICES_FILE_PATH"] = full_files["invoice"]
        os.environ["SALES_RECEIPTS_FILE_PATH"] = full_files["sales_receipt"]
    else:
        # Use the default paths from the original script
        logger.info("Importing full dataset from default paths")
        os.environ["ITEMS_FILE_PATH"] = "/Users/oren/Dropbox-AAC/AAC/Oren/CSV/item_all.csv"
        os.environ["CUSTOMERS_FILE_PATH"] = "/Users/oren/Dropbox-AAC/AAC/Oren/CSV/customers_all.csv"
        os.environ["INVOICES_FILE_PATH"] = "/Users/oren/Dropbox-AAC/AAC/Oren/CSV/invoice_all.csv"
        os.environ["SALES_RECEIPTS_FILE_PATH"] = "/Users/oren/Dropbox-AAC/AAC/Oren/CSV/sales_all.csv"
    
    # Drop schemas and run full refresh
    logger.info("Dropping schemas and running full refresh")
    result = run_command("meltano run --full-refresh tap-csv target-postgres", dry_run)
    if result != 0:
        logger.error("Full dataset import failed")
        return result
    
    # Run DBT build to handle all dependencies (models, snapshots, etc.)
    logger.info("Running DBT build to create models and snapshots")
    result = run_command("meltano invoke dbt-postgres:build", dry_run)
    if result != 0:
        logger.error("DBT build failed")
        return result
    
    # Run the matcher
    logger.info("Running matcher")
    result = run_command("./matcher.py", dry_run)
    if result != 0:
        logger.error("Matcher failed")
        return result
    
    logger.info("Full import process completed successfully")
    return 0

def parse_date_from_filename(filename):
    """Extract date from filename pattern like Invoice_MM_DD_YYYY_H_MM_SS.csv."""
    # Match pattern like Invoice_03_23_2025_2_00_03.csv or Sales Receipt_03_23_2025_3_00_03.csv
    pattern = r'(?:Invoice|Sales Receipt|Item|Customer)_(\d{2})_(\d{2})_(\d{4})_\d+_\d+_\d+\.csv'
    match = re.match(pattern, os.path.basename(filename))
    
    if match:
        month, day, year = match.groups()
        try:
            return datetime(int(year), int(month), int(day))
        except ValueError:
            logger.warning(f"Invalid date in filename: {filename}")
            return None
    
    logger.warning(f"Could not parse date from filename: {filename}")
    return None

def group_files_by_date(directory):
    """Group files in the directory by their date."""
    files_by_date = {}
    
    # Get all CSV files in the directory
    csv_files = glob.glob(os.path.join(directory, "*.csv"))
    
    for file_path in csv_files:
        date = parse_date_from_filename(file_path)
        if date:
            date_str = date.strftime("%Y-%m-%d")
            if date_str not in files_by_date:
                files_by_date[date_str] = {}
            
            # Determine file type
            filename = os.path.basename(file_path)
            if filename.startswith("Invoice_"):
                files_by_date[date_str]["invoice"] = file_path
            elif filename.startswith("Sales Receipt_"):
                files_by_date[date_str]["sales_receipt"] = file_path
            elif filename.startswith("Item_"):
                files_by_date[date_str]["item"] = file_path
            elif filename.startswith("Customer_"):
                files_by_date[date_str]["customer"] = file_path
    
    return files_by_date

def get_db_connection():
    """Get a connection to the PostgreSQL database."""
    conn = psycopg2.connect(
        host="localhost",
        database="mqi",
        user="aac",
        password=os.getenv("TARGET_POSTGRES_PASSWORD")
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    return conn

def initialize_imported_files_table():
    """Create the imported_files table if it doesn't exist."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # First ensure the raw schema exists
        cursor.execute("CREATE SCHEMA IF NOT EXISTS raw")
        
        # Create the table in the raw schema
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw.imported_files (
                id SERIAL PRIMARY KEY,
                filename TEXT NOT NULL,
                file_path TEXT NOT NULL,
                file_type TEXT NOT NULL,
                import_date TIMESTAMP NOT NULL DEFAULT NOW(),
                status TEXT NOT NULL,
                file_date DATE
            )
        """)
        logger.info("Initialized imported_files table in raw schema")
    except Exception as e:
        logger.error(f"Error initializing imported_files table: {str(e)}")
    finally:
        cursor.close()
        conn.close()

def is_file_processed(file_path):
    """Check if a file has already been processed by querying the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT status FROM raw.imported_files WHERE file_path = %s",
            (file_path,)
        )
        result = cursor.fetchone()
        return result is not None
    except Exception as e:
        logger.error(f"Error checking if file is processed: {str(e)}")
        return False
    finally:
        cursor.close()
        conn.close()

def record_file_import(file_path, file_type, status, file_date=None):
    """Record a file import in the database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        filename = os.path.basename(file_path)
        cursor.execute(
            """
            INSERT INTO raw.imported_files 
            (filename, file_path, file_type, status, file_date)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (filename, file_path, file_type, status, file_date)
        )
        logger.info(f"Recorded {status} import for {filename}")
    except Exception as e:
        logger.error(f"Error recording file import: {str(e)}")
    finally:
        cursor.close()
        conn.close()

def process_daily_files(directory, move_files=False, archive=False, dry_run=False, test_mode=False):
    """Process files from directory in date sequence."""
    logger.info(f"Starting daily import from parent directory: {directory}")
    
    # Initialize the imported_files table
    if not dry_run and not test_mode:
        initialize_imported_files_table()
    
    # Define input directory
    input_dir = os.path.join(directory, "input")
    
    # Check if input directory exists
    if not os.path.isdir(input_dir):
        logger.error(f"Input directory does not exist: {input_dir}")
        return 1
    
    # Group files by date
    files_by_date = group_files_by_date(input_dir)
    
    if not files_by_date:
        logger.warning("No valid files found in directory")
        return 0
    
    # Sort dates
    sorted_dates = sorted(files_by_date.keys())
    logger.info(f"Found files for {len(sorted_dates)} dates: {', '.join(sorted_dates)}")
    
    # In test mode, show more detailed information about what would be processed
    if test_mode:
        logger.info("\nTEST MODE: Showing files that would be processed")
        logger.info("=" * 80)
        
        for date_str in sorted_dates:
            files = files_by_date[date_str]
            logger.info(f"\nDate: {date_str}")
            
            # Check for required file types
            all_types_present = True
            for file_type in ["item", "customer", "invoice", "sales_receipt"]:
                if file_type in files:
                    file_path = files[file_type]
                    filename = os.path.basename(file_path)
                    processed = is_file_processed(file_path) if not dry_run else False
                    status = "ALREADY PROCESSED" if processed else "WILL PROCESS"
                    logger.info(f"  ✓ {file_type.capitalize()}: {filename} ({status})")
                else:
                    logger.info(f"  ✗ {file_type.capitalize()}: MISSING")
                    all_types_present = False
            
            if all_types_present:
                logger.info(f"  Status: WILL PROCESS (all required files present)")
            else:
                logger.info(f"  Status: WILL SKIP (missing required files)")
        
        logger.info("\n" + "=" * 80)
        return 0
    
    # Track if we processed any files successfully
    processed_any = False
    
    # Process each date
    for date_str in sorted_dates:
        files = files_by_date[date_str]
        logger.info(f"Processing files for date: {date_str}")
        
        # Check if we have all required file types
        missing_types = []
        for file_type in ["item", "customer", "invoice", "sales_receipt"]:
            if file_type not in files:
                missing_types.append(file_type)
        
        if missing_types:
            logger.warning(f"Missing file types for {date_str}: {', '.join(missing_types)}")
            logger.warning(f"Skipping date: {date_str}")
            continue
        
        # Check if all files have already been processed
        all_processed = True
        for file_type in ["item", "customer", "invoice", "sales_receipt"]:
            file_path = files[file_type]
            if not is_file_processed(file_path):
                all_processed = False
                break
        
        if all_processed:
            logger.info(f"All files for {date_str} have already been processed. Skipping.")
            continue
        
        # Set environment variables for file paths
        os.environ["ITEMS_FILE_PATH"] = files["item"]
        os.environ["CUSTOMERS_FILE_PATH"] = files["customer"]
        os.environ["INVOICES_FILE_PATH"] = files["invoice"]
        os.environ["SALES_RECEIPTS_FILE_PATH"] = files["sales_receipt"]
        
        # Run Meltano pipeline
        logger.info(f"Importing data for {date_str}")
        result = run_command("meltano run tap-csv target-postgres", dry_run)
        if result != 0:
            logger.error(f"Import failed for date: {date_str}")
            
            # Record failed imports in the database
            if not dry_run and not test_mode:
                file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                for file_type in ["item", "customer", "invoice", "sales_receipt"]:
                    if file_type in files:
                        file_path = files[file_type]
                        record_file_import(file_path, file_type, "failed", file_date)
            
            continue
        
        # Run DBT build to handle all dependencies (models, snapshots, etc.)
        logger.info(f"Running DBT build for {date_str}")
        result = run_command("meltano invoke dbt-postgres:build", dry_run)
        if result != 0:
            logger.error(f"DBT build failed for date: {date_str}")
            
            # Record failed imports in the database
            if not dry_run and not test_mode:
                file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                for file_type in ["item", "customer", "invoice", "sales_receipt"]:
                    if file_type in files:
                        file_path = files[file_type]
                        record_file_import(file_path, file_type, "failed", file_date)
            
            continue
        
        logger.info(f"Successfully processed files for date: {date_str}")
        processed_any = True
        
        # Record successful imports in the database
        if not dry_run and not test_mode:
            file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            for file_type in ["item", "customer", "invoice", "sales_receipt"]:
                if file_type in files:
                    file_path = files[file_type]
                    record_file_import(file_path, file_type, "success", file_date)
        
        # Handle archived files if requested
        if archive:
            logger.info("File archiving not implemented yet")
            # TODO: Implement file archiving logic
    
    # Run matcher once at the end if any files were processed
    if processed_any:
        logger.info("All imports complete. Running matcher...")
        result = run_command("./matcher.py", dry_run)
        if result != 0:
            logger.error("Matcher failed")
            return result
        logger.info("Matcher completed successfully")
    else:
        logger.warning("No files were processed, skipping matcher")
    
    logger.info("Daily import process completed")
    return 0

def test_daily_import(directory):
    """Test the daily import process without executing any commands."""
    logger.info("TEST MODE: Checking daily import files")
    logger.info("=" * 80)
    
    # Define directory structure
    input_dir = os.path.join(directory, "input")
    processed_dir = os.path.join(directory, "processed")
    failed_dir = os.path.join(directory, "failed")
    
    # Check if input directory exists
    if not os.path.isdir(input_dir):
        logger.error(f"Input directory does not exist: {input_dir}")
        return 1
    
    logger.info(f"Directory structure:")
    logger.info(f"  Input directory: {input_dir}")
    logger.info(f"  Processed directory: {processed_dir} (will be created if needed)")
    logger.info(f"  Failed directory: {failed_dir} (will be created if needed)")
    
    # Group files by date
    files_by_date = group_files_by_date(input_dir)
    
    if not files_by_date:
        logger.warning("No valid files found in input directory")
        return 0
    
    # Sort dates
    sorted_dates = sorted(files_by_date.keys())
    logger.info(f"Found files for {len(sorted_dates)} dates: {', '.join(sorted_dates)}")
    
    # Show detailed information about what would be processed
    logger.info("\nFiles that would be processed:")
    for date_str in sorted_dates:
        files = files_by_date[date_str]
        logger.info(f"\nDate: {date_str}")
        
        # Check for required file types
        all_types_present = True
        for file_type in ["item", "customer", "invoice", "sales_receipt"]:
            if file_type in files:
                logger.info(f"  ✓ {file_type.capitalize()}: {os.path.basename(files[file_type])}")
            else:
                logger.info(f"  ✗ {file_type.capitalize()}: MISSING")
                all_types_present = False
        
        if all_types_present:
            logger.info(f"  Status: WILL PROCESS (all required files present)")
            logger.info(f"  After processing: Files will be moved to {processed_dir}")
        else:
            logger.info(f"  Status: WILL SKIP (missing required files)")
    
    logger.info("\n" + "=" * 80)
    return 0

def test_full_import(directory):
    """Test the full import process without executing any commands."""
    logger.info("TEST MODE: Checking full import files")
    logger.info("=" * 80)
    
    if directory:
        logger.info(f"Looking for full dataset files in: {directory}")
        
        # Check if directory exists
        if not os.path.isdir(directory):
            logger.error(f"Directory does not exist: {directory}")
            return 1
        
        # Get all CSV files in the directory
        csv_files = glob.glob(os.path.join(directory, "*.csv"))
        logger.info(f"Found {len(csv_files)} CSV files in directory")
        
        # Group files by type
        full_files = {
            "item": [],
            "customer": [],
            "invoice": [],
            "sales_receipt": []
        }
        
        for file_path in csv_files:
            filename = os.path.basename(file_path)
            
            # Check for full dataset files
            for file_type in ["item", "customer", "invoice", "sales_receipt"]:
                pattern = rf"{file_type}_all_(\d{{2}})_(\d{{2}})_(\d{{4}})\.csv"
                if re.match(pattern, filename):
                    full_files[file_type].append(file_path)
        
        # Display results
        logger.info("\nFull dataset files found:")
        all_types_present = True
        for file_type in ["item", "customer", "invoice", "sales_receipt"]:
            if full_files[file_type]:
                files_str = ", ".join([os.path.basename(f) for f in full_files[file_type]])
                logger.info(f"  ✓ {file_type.capitalize()}: {files_str}")
            else:
                logger.info(f"  ✗ {file_type.capitalize()}: MISSING")
                all_types_present = False
        
        if all_types_present:
            logger.info("\nStatus: WILL PROCESS (all required files present)")
        else:
            logger.info("\nStatus: WILL SKIP (missing required files)")
    else:
        logger.info("Using default file paths:")
        logger.info(f"  ITEMS_FILE_PATH: /Users/oren/Dropbox-AAC/AAC/Oren/CSV/item_all.csv")
        logger.info(f"  CUSTOMERS_FILE_PATH: /Users/oren/Dropbox-AAC/AAC/Oren/CSV/customers_all.csv")
        logger.info(f"  INVOICES_FILE_PATH: /Users/oren/Dropbox-AAC/AAC/Oren/CSV/invoice_all.csv")
        logger.info(f"  SALES_RECEIPTS_FILE_PATH: /Users/oren/Dropbox-AAC/AAC/Oren/CSV/sales_all.csv")
    
    logger.info("\nCommands that would be executed:")
    logger.info("  1. meltano run --full-refresh tap-csv target-postgres (drops schemas and imports data)")
    logger.info("  2. meltano invoke dbt-postgres:build (runs DBT build to create models and snapshots)")
    logger.info("  3. ./matcher.py")
    
    logger.info("=" * 80)
    return 0

def main():
    """Main entry point for the script."""
    args = parse_arguments()
    
    # Check if directory is provided
    if not args.dir and args.daily:
        logger.error("Directory must be specified with --dir when using --daily")
        return 1
    
    try:
        # Initialize the imported_files table if not in test mode
        if not args.test and not args.dry_run:
            initialize_imported_files_table()
            
        if args.full:
            if args.test:
                return test_full_import(args.dir)
            else:
                # Use the directory if provided
                return run_full_import(args.dir, args.dry_run)
        elif args.daily:
            if args.test:
                return test_daily_import(args.dir)
            else:
                return process_daily_files(args.dir, True, args.archive, 
                                          args.dry_run, test_mode=False)
    except Exception as e:
        logger.error(f"Unhandled exception: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

if __name__ == "__main__":
    sys.exit(main())

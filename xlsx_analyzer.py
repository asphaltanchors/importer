# ABOUTME: Temporary script to analyze XLSX file structure for migration planning
# ABOUTME: Examines worksheet tabs, schemas, and data content for QuickBooks XLSX exports

import os
import sys
from pathlib import Path
import pandas as pd
import openpyxl
from dotenv import load_dotenv

# Load environment
load_dotenv()

def analyze_xlsx_file(file_path):
    """Analyze structure and content of an XLSX file"""
    print(f"\n{'='*60}")
    print(f"ANALYZING: {os.path.basename(file_path)}")
    print(f"{'='*60}")
    
    if not os.path.exists(file_path):
        print(f"ERROR: File not found: {file_path}")
        return
    
    # Get file size
    file_size = os.path.getsize(file_path)
    print(f"File size: {file_size:,} bytes ({file_size/1024/1024:.2f} MB)")
    
    # Load workbook to get sheet names
    try:
        workbook = openpyxl.load_workbook(file_path, read_only=True)
        sheet_names = workbook.sheetnames
        print(f"Number of worksheets: {len(sheet_names)}")
        print(f"Worksheet names: {sheet_names}")
        workbook.close()
    except Exception as e:
        print(f"ERROR loading workbook: {e}")
        return
    
    # Analyze each worksheet
    for sheet_name in sheet_names:
        print(f"\n{'-'*40}")
        print(f"WORKSHEET: {sheet_name}")
        print(f"{'-'*40}")
        
        try:
            # Read first few rows to understand structure
            df = pd.read_excel(file_path, sheet_name=sheet_name, nrows=5)
            
            print(f"Columns ({len(df.columns)}): {list(df.columns)}")
            print(f"Data types:")
            for col, dtype in df.dtypes.items():
                print(f"  {col}: {dtype}")
            
            print(f"\nFirst 3 rows:")
            if len(df) > 0:
                for idx, row in df.head(3).iterrows():
                    print(f"  Row {idx + 1}: {dict(row)}")
            else:
                print("  No data rows found")
                
            # Get total row count
            full_df = pd.read_excel(file_path, sheet_name=sheet_name)
            print(f"Total rows: {len(full_df)}")
            
            # Look for key identifying columns that might match our current CSV structure
            key_columns = []
            for col in df.columns:
                col_lower = col.lower()
                if any(key in col_lower for key in ['id', 'name', 'number', 'date', 'customer', 'item', 'product', 'service']):
                    key_columns.append(col)
            
            if key_columns:
                print(f"Key columns (potential identifiers): {key_columns}")
                
        except Exception as e:
            print(f"ERROR reading worksheet '{sheet_name}': {e}")

def main():
    # Get DROPBOX_PATH from environment
    dropbox_path = os.environ.get('DROPBOX_PATH')
    if not dropbox_path:
        print("ERROR: DROPBOX_PATH not set in environment")
        sys.exit(1)
    
    # Parent directory should contain the sample XLSX files
    parent_dir = Path(dropbox_path).parent
    
    print(f"Looking for XLSX files in: {parent_dir}")
    
    # Analyze both XLSX files
    xlsx_files = [
        parent_dir / "all-list-test.xlsx",
        parent_dir / "all-transaction-test.xlsx"
    ]
    
    for xlsx_file in xlsx_files:
        analyze_xlsx_file(str(xlsx_file))
    
    print(f"\n{'='*60}")
    print("ANALYSIS COMPLETE")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
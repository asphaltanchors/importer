"""Invoice validation processor."""

from pathlib import Path
from typing import Dict, Any, List
import csv
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from ..db.session import SessionManager
from ..db.models.customer import Customer
from .sales_validator import validate_sales_file

def validate_invoice_file(file_path: Path, database_url: str) -> Dict[str, Any]:
    """Validate an invoice CSV file.
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        Dict containing validation results with structure:
        {
            'is_valid': bool,
            'summary': {
                'stats': {
                    'total_rows': int,
                    'valid_rows': int,
                    'rows_with_warnings': int,
                    'rows_with_errors': int
                },
                'errors': List[Dict]
            }
        }
    """
    # First run base sales validation
    results = validate_sales_file(file_path)
    
    if not results['is_valid']:
        return results
        
    try:
        with open(file_path, 'r') as f:
            reader = csv.DictReader(f)
            
            # Verify this is an invoice file by checking header
            if 'Invoice No' not in reader.fieldnames:
                results['is_valid'] = False
                results['summary']['errors'].append({
                    'row': 0,
                    'severity': 'CRITICAL',
                    'field': 'file_type',
                    'message': "Not an invoice file - 'Invoice No' field not found"
                })
                return results
                
            # Get customer names for validation
            session_manager = SessionManager(database_url)
            with session_manager as session:
                customer_names = set(
                    name for (name,) in 
                    session.execute(select(Customer.customerName))
                )
            
            # Additional invoice-specific validation
            for row_num, row in enumerate(reader, start=1):
                customer_name = row.get('Customer', '').strip()
                
                # Skip empty rows and special items
                if (not customer_name or
                    not row.get('Product/Service', '').strip() or
                    not row.get('Product/Service Amount', '').strip()):
                    continue
                
                # Validate customer exists
                if customer_name and customer_name not in customer_names:
                    results['summary']['stats']['rows_with_errors'] += 1
                    results['is_valid'] = False
                    results['summary']['errors'].append({
                        'row': row_num,
                        'severity': 'CRITICAL',
                        'field': 'customer',
                        'message': f"Customer not found in database: {customer_name}"
                    })
                
                # Validate invoice-specific fields
                if not row.get('Invoice No', '').strip():
                    results['summary']['stats']['rows_with_errors'] += 1
                    results['is_valid'] = False
                    results['summary']['errors'].append({
                        'row': row_num,
                        'severity': 'CRITICAL',
                        'field': 'invoice_number',
                        'message': "Missing invoice number"
                    })
                
                # Validate payment terms if present
                terms = row.get('Terms', '').strip()
                if terms and not any(term in terms.lower() for term in ['net', 'due', 'cod', 'prepaid']):
                    results['summary']['stats']['rows_with_warnings'] += 1
                    results['summary']['errors'].append({
                        'row': row_num,
                        'severity': 'WARNING',
                        'field': 'terms',
                        'message': f"Unusual payment terms: {terms}"
                    })
                    
    except Exception as e:
        results['is_valid'] = False
        results['summary']['errors'].append({
            'row': 0,
            'severity': 'CRITICAL',
            'field': 'file',
            'message': f"Failed to process file: {str(e)}"
        })
    
    return results

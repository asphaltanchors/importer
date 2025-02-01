"""Sales data validation processor."""

from pathlib import Path
from typing import Dict, Any, List
import csv
from datetime import datetime

def validate_sales_file(file_path: Path) -> Dict[str, Any]:
    """Validate a sales CSV file.
    
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
    results = {
        'is_valid': True,
        'summary': {
            'stats': {
                'total_rows': 0,
                'valid_rows': 0,
                'rows_with_warnings': 0,
                'rows_with_errors': 0
            },
            'errors': []
        }
    }
    
    # Map field names for both invoice and sales receipt formats
    field_mappings = {
        'transaction_number': ['Invoice No', 'Sales Receipt No'],
        'transaction_date': ['Invoice Date', 'Sales Receipt Date'],
        'customer_name': ['Customer'],
        'item_code': ['Product/Service'],
        'description': ['Product/Service Description'],
        'quantity': ['Product/Service Quantity'],
        'unit_price': ['Product/Service Rate'],
        'amount': ['Product/Service  Amount', 'Product/Service Amount']  # Handle both single and double space variants
    }

    # Fields that must be present in the file
    required_fields = [
        'transaction_number',
        'transaction_date',
        'customer_name',
        'item_code',
        'quantity',
        'unit_price',
        'amount'
    ]
    
    try:
        with open(file_path, 'r') as f:
            reader = csv.DictReader(f)
            
            # Map CSV headers to our standardized field names
            header_mapping = {}
            missing_fields = []
            for std_field, possible_names in field_mappings.items():
                found = False
                for name in possible_names:
                    if name in reader.fieldnames:
                        header_mapping[std_field] = name
                        found = True
                        break
                if not found and std_field in required_fields:
                    missing_fields.append(std_field)
            if missing_fields:
                results['is_valid'] = False
                results['summary']['errors'].append({
                    'row': 0,
                    'severity': 'CRITICAL',
                    'field': 'headers',
                    'message': f"Missing required fields: {', '.join(missing_fields)}"
                })
                return results
            
            for row_num, row in enumerate(reader, start=1):
                results['summary']['stats']['total_rows'] += 1
                row_has_error = False
                row_has_warning = False
                
                # Skip validation for shipping, tax, discount, and empty rows
                item_code = row[header_mapping['item_code']].strip().lower()
                amount = row[header_mapping['amount']].strip()
                
                # Skip empty rows (used for formatting)
                if not item_code and not amount:
                    results['summary']['stats']['valid_rows'] += 1
                    continue
                    
                # Skip special rows
                if (item_code == 'shipping' or
                    'tax' in item_code or
                    'discount' in item_code):
                    results['summary']['stats']['valid_rows'] += 1
                    continue

                # Validate required fields presence using mapped headers
                for field in required_fields:
                    mapped_field = header_mapping.get(field)
                    if not mapped_field or not row[mapped_field].strip():
                        row_has_error = True
                        results['summary']['errors'].append({
                            'row': row_num,
                            'severity': 'CRITICAL',
                            'field': field,
                            'message': f"Missing required value"
                        })
                
                # Validate transaction date format
                try:
                    date_field = header_mapping['transaction_date']
                    if row[date_field].strip():
                        # Try multiple date formats
                        date_str = row[date_field].strip()
                        try:
                            datetime.strptime(date_str, '%Y-%m-%d')
                        except ValueError:
                            try:
                                datetime.strptime(date_str, '%m-%d-%Y')
                            except ValueError:
                                raise ValueError("Invalid date format")
                except ValueError:
                    row_has_error = True
                    results['summary']['errors'].append({
                        'row': row_num,
                        'severity': 'CRITICAL',
                        'field': 'transaction_date',
                        'message': "Invalid date format. Expected YYYY-MM-DD"
                    })
                
                # Validate numeric fields
                numeric_fields = ['quantity', 'unit_price', 'amount']
                for field in numeric_fields:
                    mapped_field = header_mapping[field]
                    if row[mapped_field].strip():
                        try:
                            value = float(row[mapped_field])
                            if value < 0:
                                row_has_warning = True
                                results['summary']['errors'].append({
                                    'row': row_num,
                                    'severity': 'WARNING',
                                    'field': field,
                                    'message': f"Negative value: {value}"
                                })
                        except ValueError:
                            row_has_error = True
                            results['summary']['errors'].append({
                                'row': row_num,
                                'severity': 'CRITICAL',
                                'field': field,
                                'message': f"Invalid numeric value: {row[mapped_field]}"
                            })
                
                # Validate amount calculation
                try:
                    quantity = float(row[header_mapping['quantity']])
                    unit_price = float(row[header_mapping['unit_price']])
                    amount = float(row[header_mapping['amount']])
                    calculated_amount = round(quantity * unit_price, 2)
                    
                    if abs(calculated_amount - amount) > 0.01:  # Allow for small rounding differences
                        row_has_warning = True
                        results['summary']['errors'].append({
                            'row': row_num,
                            'severity': 'WARNING',
                            'field': 'amount',
                            'message': f"Amount {amount} does not match quantity * unit_price = {calculated_amount}"
                        })
                except (ValueError, TypeError):
                    # Skip amount validation if any numeric parsing failed
                    pass
                
                # Update statistics
                if row_has_error:
                    results['summary']['stats']['rows_with_errors'] += 1
                    results['is_valid'] = False
                elif row_has_warning:
                    results['summary']['stats']['rows_with_warnings'] += 1
                    results['summary']['stats']['valid_rows'] += 1
                else:
                    results['summary']['stats']['valid_rows'] += 1
                    
    except Exception as e:
        results['is_valid'] = False
        results['summary']['errors'].append({
            'row': 0,
            'severity': 'CRITICAL',
            'field': 'file',
            'message': f"Failed to process file: {str(e)}"
        })
    
    return results

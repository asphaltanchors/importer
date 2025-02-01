from typing import Dict, List, Optional
import csv
from dataclasses import dataclass
from pathlib import Path

@dataclass
class ValidationError:
    row_number: int
    field: str
    message: str
    severity: str  # 'CRITICAL', 'WARNING', 'INFO'

class CustomerDataValidator:
    """Validates customer data CSV files before processing."""
    
    REQUIRED_FIELDS = [
        'Customer Name',  # Maps to customerName
        'QuickBooks Internal Id'  # For duplicate detection
    ]

    EMAIL_FIELDS = [
        'Main Email',
        'CC Email',
        'Work Email',
        'Notes',  # Sometimes contains email addresses
        'Additional Notes'
    ]

    CANADIAN_PROVINCES = [
        'AB', 'BC', 'MB', 'NB', 'NL', 'NS', 'NT', 
        'NU', 'ON', 'PE', 'QC', 'SK', 'YT'
    ]

    def __init__(self, csv_path: Path):
        self.csv_path = csv_path
        self.errors: List[ValidationError] = []
        self.stats = {
            'total_rows': 0,
            'valid_rows': 0,
            'rows_with_warnings': 0,
            'rows_with_errors': 0
        }

    def validate_file_structure(self) -> bool:
        """Validates basic CSV structure and required fields existence."""
        try:
            with open(self.csv_path, 'r', encoding='utf-8') as f:
                # Read header
                reader = csv.DictReader(f)
                header = reader.fieldnames

                if not header:
                    self.errors.append(
                        ValidationError(
                            row_number=0,
                            field='FILE',
                            message='CSV file has no header row',
                            severity='CRITICAL'
                        )
                    )
                    return False

                # Check required fields
                missing_fields = [
                    field for field in self.REQUIRED_FIELDS 
                    if field not in header
                ]
                
                if missing_fields:
                    self.errors.append(
                        ValidationError(
                            row_number=0,
                            field='HEADERS',
                            message=f'Missing required fields: {", ".join(missing_fields)}',
                            severity='CRITICAL'
                        )
                    )
                    return False

                # Validate each row
                for row_num, row in enumerate(reader, start=1):
                    self.stats['total_rows'] += 1
                    row_valid = self.validate_row(row_num, row)
                    
                    if row_valid:
                        self.stats['valid_rows'] += 1

            return len([e for e in self.errors if e.severity == 'CRITICAL']) == 0

        except Exception as e:
            self.errors.append(
                ValidationError(
                    row_number=0,
                    field='FILE',
                    message=f'Failed to read CSV file: {str(e)}',
                    severity='CRITICAL'
                )
            )
            return False

    def validate_row(self, row_num: int, row: Dict[str, str]) -> bool:
        """Validates a single row of customer data."""
        row_valid = True
        has_warning = False

        # Check base required fields
        for field in self.REQUIRED_FIELDS:
            if not row.get(field, '').strip():
                self.errors.append(
                    ValidationError(
                        row_number=row_num,
                        field=field,
                        message=f'Missing required field: {field}',
                        severity='CRITICAL'
                    )
                )
                row_valid = False

        # Check for incomplete addresses
        address_fields = {
            'line1': row.get('Billing Address Line 1', '').strip(),
            'city': row.get('Billing Address City', '').strip(),
            'state': row.get('Billing Address State', '').strip(),
            'postal': row.get('Billing Address Postal Code', '').strip(),
            'country': row.get('Billing Address Country', '').strip()
        }
        
        # If any address field is provided, check if others are missing
        if any(address_fields.values()):
            missing = [k for k, v in address_fields.items() if not v]
            if missing:
                self.errors.append(
                    ValidationError(
                        row_number=row_num,
                        field='Billing Address',
                        message=f'Incomplete address: missing {", ".join(missing)}',
                        severity='WARNING'
                    )
                )
                has_warning = True

        # Enhanced email validation
        valid_email_found = False
        for field in self.EMAIL_FIELDS:
            value = row.get(field, '').strip()
            if value:
                # Split on common separators and look for emails
                potential_emails = [e.strip() for e in value.replace(';', ',').split(',')]
                for email in potential_emails:
                    if '@' in email and '.' in email.split('@')[1]:  # Basic email format check
                        valid_email_found = True
                        break
            if valid_email_found:
                break

        if not valid_email_found:
            # Check phone/fax fields for emails (sometimes misplaced)
            for field in ['Main Phone', 'Alt. Phone', 'Work Phone', 'Mobile', 'Fax']:
                value = row.get(field, '').strip()
                if '@' in value and '.' in value.split('@')[1]:
                    valid_email_found = True
                    self.errors.append(
                        ValidationError(
                            row_number=row_num,
                            field=field,
                            message=f'Email found in {field} field',
                            severity='WARNING'
                        )
                    )
                    break

        if not valid_email_found:
            self.errors.append(
                ValidationError(
                    row_number=row_num,
                    field='Email',
                    message='No valid email address found in any field',
                    severity='WARNING'
                )
            )
            has_warning = True

        # Check for identical billing/shipping addresses
        billing_fields = [f for f in row.keys() if f.startswith('Billing Address')]
        shipping_fields = [f for f in row.keys() if f.startswith('Shipping Address')]
        
        identical = True
        for b, s in zip(billing_fields, shipping_fields):
            if row.get(b, '').strip() != row.get(s, '').strip():
                identical = False
                break
                
        if identical:
            self.errors.append(
                ValidationError(
                    row_number=row_num,
                    field='Address',
                    message='Identical billing and shipping addresses detected',
                    severity='INFO'
                )
            )
            has_warning = True

        # Update stats
        if not row_valid:
            self.stats['rows_with_errors'] += 1
        elif has_warning:
            self.stats['rows_with_warnings'] += 1

        return row_valid

    def get_validation_summary(self) -> Dict:
        """Returns a summary of the validation results."""
        return {
            'stats': self.stats,
            'errors': [
                {
                    'row': e.row_number,
                    'field': e.field,
                    'message': e.message,
                    'severity': e.severity
                }
                for e in self.errors
            ]
        }

def validate_customer_file(file_path: Path) -> Dict:
    """Main entry point for customer data validation."""
    validator = CustomerDataValidator(file_path)
    is_valid = validator.validate_file_structure()
    
    return {
        'is_valid': is_valid,
        'summary': validator.get_validation_summary()
    }

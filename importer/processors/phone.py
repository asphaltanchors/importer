"""Processor for handling customer phone data."""
import re
import uuid
from typing import Dict, Any, List, Tuple
import pandas as pd
from sqlalchemy.orm import Session

from ..db.models import Customer, CustomerPhone
from ..db.session import SessionManager
from .base import BaseProcessor

class PhoneProcessor(BaseProcessor[Dict[str, Any]]):
    """Process and store customer phone information."""

    def __init__(
        self,
        config: Dict[str, Any],
        batch_size: int = 100,
        error_limit: int = 1000,
        debug: bool = False
    ):
        """Initialize processor with configuration.
        
        Args:
            config: Configuration dictionary containing database_url
            batch_size: Number of records to process per batch
            error_limit: Maximum number of errors before stopping
            debug: Enable debug logging
        """
        session_manager = SessionManager(config['database_url'])
        super().__init__(session_manager, batch_size, error_limit, debug)
        
        # Match phone patterns with optional extension
        self.phone_pattern = re.compile(r'[\d\(\)\-\.\s]+(?:\s*(?:x|ext\.?)\s*\d+)?', re.IGNORECASE)
        self.extension_pattern = re.compile(r'\s*(?:x|ext\.?)\s*(\d+)', re.IGNORECASE)
        
        # Add phone-specific stats
        self.stats.phones_processed = 0
        self.stats.phones_stored = 0

    def validate_data(self, df: pd.DataFrame) -> Tuple[List[str], List[str]]:
        """Validate data before processing.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (critical_issues, warnings)
        """
        critical_issues = []
        warnings = []
        
        # Check for required columns
        required_columns = ['customer_id']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            critical_issues.append(f"Missing required columns: {', '.join(missing_columns)}")
        
        # Check for phone fields
        phone_fields = ['Main Phone', 'Alt. Phone', 'Work Phone', 'Mobile', 'Fax']
        if not any(field in df.columns for field in phone_fields):
            warnings.append("No phone fields found in data")
        
        return critical_issues, warnings

    def _clean_phone(self, phone_str: str) -> str:
        """Clean phone number, preserving extension if present."""
        if not phone_str:
            return ""
        
        phone_str = phone_str.strip()
        
        # Extract extension if present
        extension = ""
        ext_match = self.extension_pattern.search(phone_str)
        if ext_match:
            extension = f" x{ext_match.group(1)}"
            phone_str = phone_str[:ext_match.start()]
        
        # Clean main phone number (keep only digits)
        main_number = re.sub(r'[^\d]', '', phone_str)
        
        # Format based on length
        if len(main_number) == 10:
            formatted = f"({main_number[:3]}) {main_number[3:6]}-{main_number[6:]}"
        elif len(main_number) == 11 and main_number.startswith('1'):
            formatted = f"({main_number[1:4]}) {main_number[4:7]}-{main_number[7:]}"
        else:
            formatted = main_number  # Keep as-is if doesn't match expected formats
        
        return formatted + extension

    def _determine_phone_type(self, field_name: str) -> str:
        """Determine phone type based on field name."""
        field_type_map = {
            'Main Phone': 'MAIN',
            'Mobile': 'MOBILE',
            'Work Phone': 'WORK',
            'Alt. Phone': 'OTHER',
            'Fax': 'OTHER'
        }
        return field_type_map.get(field_name, 'OTHER')

    def _split_phones(self, phone_str: str) -> List[str]:
        """Split phone string on common separators and clean each number."""
        if not phone_str:
            return []
        
        # Split on common separators
        phones = re.split(r'[;,/]', phone_str)
        
        # Clean and validate each phone
        valid_phones = []
        for phone in phones:
            if self.phone_pattern.match(phone):
                cleaned = self._clean_phone(phone)
                if cleaned:  # Only add if cleaning produced a result
                    valid_phones.append(cleaned)
        
        return valid_phones

    def _process_batch(self, session: Session, batch_df: pd.DataFrame) -> pd.DataFrame:
        """Process a batch of customer phone records.
        
        Args:
            session: Database session for this batch
            batch_df: DataFrame containing the batch data
            
        Returns:
            Processed DataFrame
        """
        phone_fields = [
            'Main Phone',
            'Alt. Phone',
            'Work Phone',
            'Mobile',
            'Fax'
        ]
        
        for _, row in batch_df.iterrows():
            try:
                if pd.isna(row['customer_id']):
                    continue
                
                # Get customer record
                customer = session.query(Customer).filter_by(id=row['customer_id']).first()
                if not customer:
                    self.logger.warning(f"Customer {row['customer_id']} not found")
                    continue
                
                # Process each phone field
                for field_name in phone_fields:
                    if field_name not in row or pd.isna(row[field_name]):
                        continue
                    
                    # Split and validate phones
                    valid_phones = self._split_phones(str(row[field_name]))
                    if not valid_phones:
                        continue
                    
                    self.stats.phones_processed += len(valid_phones)
                    
                    # Create phone records
                    for i, phone in enumerate(valid_phones):
                        phone_type = self._determine_phone_type(field_name)
                        
                        # Create phone record
                        phone_record = CustomerPhone(
                            id=str(uuid.uuid4()),
                            customerId=customer.id,
                            phone=phone,
                            type=phone_type,
                            isPrimary=(i == 0)  # First phone is primary
                        )
                        
                        session.add(phone_record)
                        self.stats.phones_stored += 1
                        
            except Exception as e:
                self.logger.error(f"Error processing phone for customer {row.get('customer_id', 'unknown')}: {str(e)}")
                self.stats.total_errors += 1
                continue
        
        return batch_df

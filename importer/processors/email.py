"""Processor for handling customer email data."""
import re
import uuid
from typing import Dict, Any, List, Tuple, Optional
import pandas as pd
from sqlalchemy.orm import Session

from ..db.models import Customer, CustomerEmail
from ..db.session import SessionManager
from .base import BaseProcessor

class EmailProcessor(BaseProcessor[Dict[str, Any]]):
    """Process and store customer email information."""

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
        self.email_pattern = re.compile(r'[^@]+@[^@]+\.[^@]+')
        
        # Add email-specific stats
        self.stats.emails_processed = 0
        self.stats.emails_stored = 0

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
        
        # Check for email fields
        email_fields = ['Main Email', 'CC Email', 'Work Email']
        if not any(field in df.columns for field in email_fields):
            warnings.append("No email fields found in data")
        
        return critical_issues, warnings

    def _split_emails(self, email_str: str) -> List[str]:
        """Split email string on semicolons or commas and clean."""
        if not email_str:
            return []
        
        # Split on both semicolons and commas
        emails = re.split(r'[;,]', email_str)
        
        # Clean and validate each email
        valid_emails = []
        for email in emails:
            cleaned = email.strip().lower()
            if self.email_pattern.match(cleaned):
                valid_emails.append(cleaned)
        
        return valid_emails

    def _determine_email_type(self, is_first: bool) -> str:
        """Determine email type based on order."""
        return 'MAIN' if is_first else 'CC'

    def _process_batch(self, session: Session, batch_df: pd.DataFrame) -> pd.DataFrame:
        """Process a batch of customer email records.
        
        Args:
            session: Database session for this batch
            batch_df: DataFrame containing the batch data
            
        Returns:
            Processed DataFrame
        """
        for _, row in batch_df.iterrows():
            try:
                if pd.isna(row['customer_id']):
                    continue
                    
                # Combine all email fields
                email_fields = ['Main Email', 'CC Email', 'Work Email']
                emails = []
                for field in email_fields:
                    if field in row and pd.notna(row[field]):
                        emails.append(str(row[field]))
                email_data = ';'.join(emails)
                
                if not email_data:
                    continue

                # Get customer record
                customer = session.query(Customer).filter_by(id=row['customer_id']).first()
                if not customer:
                    self.logger.warning(f"Customer {row['customer_id']} not found")
                    continue

                # Split and validate emails
                valid_emails = self._split_emails(email_data)
                if not valid_emails:
                    continue

                self.stats.emails_processed += len(valid_emails)

                # Create email records
                for i, email in enumerate(valid_emails):
                    email_type = self._determine_email_type(i == 0)
                    
                    # Create email record
                    email_record = CustomerEmail(
                        id=str(uuid.uuid4()),
                        customerId=customer.id,
                        email=email,
                        type=email_type,
                        isPrimary=(i == 0)  # First email is primary
                    )
                    
                    session.add(email_record)
                    self.stats.emails_stored += 1

            except Exception as e:
                self.logger.error(f"Error processing email for customer {row.get('customer_id', 'unknown')}: {str(e)}")
                self.stats.total_errors += 1
                continue
        
        return batch_df

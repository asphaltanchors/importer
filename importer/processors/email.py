"""Processor for handling customer email data."""
import re
import uuid
from typing import List, Optional, Tuple

from sqlalchemy.orm import Session

from ..db.models import Customer, CustomerEmail

class EmailProcessor:
    """Process and store customer email information."""

    def __init__(self, session: Session):
        """Initialize with database session."""
        self.session = session
        self.email_pattern = re.compile(r'[^@]+@[^@]+\.[^@]+')

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

    def process_customer_emails(self, customer_id: str, email_data: str) -> Tuple[int, int]:
        """Process and store customer email information.
        
        Args:
            customer_id: The customer ID to associate emails with
            email_data: String containing one or more email addresses
            
        Returns:
            Tuple of (total emails processed, valid emails stored)
        """
        if not email_data:
            return 0, 0

        # Get customer record
        customer = self.session.query(Customer).filter_by(id=customer_id).first()
        if not customer:
            raise ValueError(f"Customer {customer_id} not found")

        # Split and validate emails
        emails = self._split_emails(email_data)
        if not emails:
            return 0, 0

        # Create email records
        stored_count = 0
        for i, email in enumerate(emails):
            email_type = self._determine_email_type(i == 0)
            
            # Create email record
            email_record = CustomerEmail(
                id=str(uuid.uuid4()),
                customerId=customer_id,
                email=email,
                type=email_type,
                isPrimary=(i == 0)  # First email is primary
            )
            
            self.session.add(email_record)
            stored_count += 1

        # Commit changes
        self.session.commit()
        
        return len(emails), stored_count

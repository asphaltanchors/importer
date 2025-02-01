"""Processor for handling customer phone data."""
import re
import uuid
from typing import List, Tuple

from sqlalchemy.orm import Session

from ..db.models import Customer, CustomerPhone

class PhoneProcessor:
    """Process and store customer phone information."""

    def __init__(self, session: Session):
        """Initialize with database session."""
        self.session = session
        # Match phone patterns with optional extension
        self.phone_pattern = re.compile(r'[\d\(\)\-\.\s]+(?:\s*(?:x|ext\.?)\s*\d+)?', re.IGNORECASE)
        self.extension_pattern = re.compile(r'\s*(?:x|ext\.?)\s*(\d+)', re.IGNORECASE)

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

    def process_customer_phones(self, customer_id: str, phone_data: str, field_name: str) -> Tuple[int, int]:
        """Process and store customer phone information.
        
        Args:
            customer_id: The customer ID to associate phones with
            phone_data: String containing one or more phone numbers
            
        Returns:
            Tuple of (total phones processed, valid phones stored)
        """
        if not phone_data:
            return 0, 0

        # Get customer record
        customer = self.session.query(Customer).filter_by(id=customer_id).first()
        if not customer:
            raise ValueError(f"Customer {customer_id} not found")

        # Split and validate phones
        phones = self._split_phones(phone_data)
        if not phones:
            return 0, 0

        # Create phone records
        stored_count = 0
        for i, phone in enumerate(phones):
            phone_type = self._determine_phone_type(field_name)
            
            # Create phone record
            phone_record = CustomerPhone(
                id=str(uuid.uuid4()),
                customerId=customer_id,
                phone=phone,
                type=phone_type,
                isPrimary=(i == 0)  # First phone is primary
            )
            
            self.session.add(phone_record)
            stored_count += 1

        # Commit changes
        self.session.commit()
        
        return len(phones), stored_count

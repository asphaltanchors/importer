"""Processor for handling customer address data."""
import hashlib
import json
from typing import Dict, Optional, Tuple
import pandas as pd
from sqlalchemy.orm import Session

from ..db.models.address import Address
from .base import BaseProcessor

class AddressProcessor(BaseProcessor):
    """Processes customer address data from CSV imports."""
    
    def __init__(self, session: Session):
        """Initialize processor with database session."""
        self.session = session
        self.address_cache: Dict[str, str] = {}  # Cache of address hash -> address id
        self.stats = {
            'addresses_processed': 0,
            'unique_addresses': 0,
            'duplicate_addresses': 0,
            'billing_addresses': 0,
            'shipping_addresses': 0,
            'identical_billing_shipping': 0
        }

    def _clean_address_field(self, value: Optional[str | int]) -> str:
        """Clean an address field value."""
        if value is None:
            return ""
        # Convert to string first to handle numeric postal codes
        return " ".join(str(value).strip().split())  # Remove extra whitespace

    def _generate_address_hash(self, address_dict: Dict[str, str]) -> str:
        """Generate a stable hash for address deduplication."""
        # Create a normalized dictionary for hashing
        normalized = {
            'line1': self._clean_address_field(address_dict.get('line1')),
            'line2': self._clean_address_field(address_dict.get('line2')),
            'line3': self._clean_address_field(address_dict.get('line3')),
            'city': self._clean_address_field(address_dict.get('city')),
            'state': self._clean_address_field(address_dict.get('state')),
            'postalCode': self._clean_address_field(address_dict.get('postalCode')),
            'country': self._clean_address_field(address_dict.get('country'))
        }
        
        # Create a stable string representation for hashing
        address_str = json.dumps(normalized, sort_keys=True)
        return hashlib.sha256(address_str.encode()).hexdigest()

    def _extract_address(self, row: pd.Series, prefix: str) -> Optional[Dict[str, str]]:
        """Extract address fields from a row with given prefix."""
        fields = {
            'line1': f'{prefix} Line 1',
            'line2': f'{prefix} Line 2',
            'line3': f'{prefix} Line 3',
            'city': f'{prefix} City',
            'state': f'{prefix} State',
            'postalCode': f'{prefix} Postal Code',
            'country': f'{prefix} Country'
        }
        
        # Check if we have any address data
        if not any(row.get(field) for field in fields.values()):
            return None
            
        return {
            key: self._clean_address_field(row.get(field))
            for key, field in fields.items()
        }

    def _process_address(self, address_dict: Dict[str, str]) -> Optional[str]:
        """Process a single address and return its ID."""
        if not address_dict or not address_dict['line1']:
            return None
            
        # Generate hash for deduplication
        address_hash = self._generate_address_hash(address_dict)
        
        # Check cache first
        if address_hash in self.address_cache:
            self.stats['duplicate_addresses'] += 1
            return self.address_cache[address_hash]
            
        # Check if address already exists in database
        existing_address = self.session.query(Address).filter_by(id=address_hash[:32]).first()
        if existing_address:
            self.stats['duplicate_addresses'] += 1
            return existing_address.id
            
        # Create new address
        address = Address(
            id=address_hash[:32],  # Use first 32 chars of hash as ID
            **address_dict
        )
        
        self.session.add(address)
        self.address_cache[address_hash] = address.id
        self.stats['unique_addresses'] += 1
        
        return address.id

    def process_row(self, row: pd.Series) -> Tuple[Optional[str], Optional[str]]:
        """Process addresses for a single row."""
        self.stats['addresses_processed'] += 1
        
        # Extract and process billing address
        billing_dict = self._extract_address(row, 'Billing Address')
        billing_id = self._process_address(billing_dict) if billing_dict else None
        if billing_id:
            self.stats['billing_addresses'] += 1
            
        # Extract and process shipping address
        shipping_dict = self._extract_address(row, 'Shipping Address')
        shipping_id = self._process_address(shipping_dict) if shipping_dict else None
        if shipping_id:
            self.stats['shipping_addresses'] += 1
            
        # Check if addresses are identical
        if billing_id and shipping_id and billing_id == shipping_id:
            self.stats['identical_billing_shipping'] += 1
            
        return billing_id, shipping_id

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process all addresses in the dataframe."""
        # Add columns for address IDs
        df['billing_address_id'] = None
        df['shipping_address_id'] = None
        
        # Process each row
        for idx, row in df.iterrows():
            billing_id, shipping_id = self.process_row(row)
            df.at[idx, 'billing_address_id'] = billing_id
            df.at[idx, 'shipping_address_id'] = shipping_id
            
        # Commit all addresses
        self.session.commit()
        
        return df

    def get_stats(self) -> Dict[str, int]:
        """Get processing statistics."""
        return self.stats

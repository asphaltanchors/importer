"""Processor for handling customer address data."""
import hashlib
import json
from typing import Dict, Optional, Tuple, List, Any
import pandas as pd
from sqlalchemy.orm import Session

from ..db.models.address import Address
from ..db.session import SessionManager
from .base import BaseProcessor

class AddressProcessor(BaseProcessor[Dict[str, Any]]):
    """Processes customer address data from CSV imports."""
    
    def __init__(
        self,
        config: Dict[str, Any],
        batch_size: int = 100,
        error_limit: int = 1000,
        debug: bool = False
    ):
        """Initialize processor with session manager.
        
        Args:
            config: Configuration dictionary containing database_url
            batch_size: Number of records to process per batch
            error_limit: Maximum number of errors before stopping
            debug: Enable debug logging
        """
        session_manager = SessionManager(config['database_url'])
        super().__init__(session_manager, batch_size, error_limit, debug)
        self.address_cache: Dict[str, str] = {}  # Cache of address hash -> address id
        
        # Add address-specific stats
        self.stats.unique_addresses = 0
        self.stats.duplicate_addresses = 0
        self.stats.billing_addresses = 0
        self.stats.shipping_addresses = 0
        self.stats.identical_billing_shipping = 0

    def validate_data(self, df: pd.DataFrame) -> Tuple[List[str], List[str]]:
        """Validate data before processing.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (critical_issues, warnings)
        """
        critical_issues = []
        warnings = []
        
        # Check for required billing address fields
        billing_fields = [
            'Billing Address Line 1',
            'Billing Address City',
            'Billing Address State',
            'Billing Address Postal Code'
        ]
        missing_billing = [f for f in billing_fields if f not in df.columns]
        if missing_billing:
            warnings.append(f"Missing billing address fields: {', '.join(missing_billing)}")
        
        # Check for required shipping address fields
        shipping_fields = [
            'Shipping Address Line 1',
            'Shipping Address City',
            'Shipping Address State',
            'Shipping Address Postal Code'
        ]
        missing_shipping = [f for f in shipping_fields if f not in df.columns]
        if missing_shipping:
            warnings.append(f"Missing shipping address fields: {', '.join(missing_shipping)}")
        
        # Check for empty required fields
        if 'Billing Address Line 1' in df.columns:
            empty_billing = df[df['Billing Address Line 1'].isna()]
            if not empty_billing.empty:
                msg = (f"Found {len(empty_billing)} rows with missing billing address line 1. "
                      f"First few row numbers: {', '.join(map(str, empty_billing.index[:3]))}")
                warnings.append(msg)
        
        if 'Shipping Address Line 1' in df.columns:
            empty_shipping = df[df['Shipping Address Line 1'].isna()]
            if not empty_shipping.empty:
                msg = (f"Found {len(empty_shipping)} rows with missing shipping address line 1. "
                      f"First few row numbers: {', '.join(map(str, empty_shipping.index[:3]))}")
                warnings.append(msg)
        
        return critical_issues, warnings

    def _clean_address_field(self, value: Optional[str | int | float]) -> str:
        """Clean an address field value."""
        if pd.isna(value):
            return ""
        # Convert to string first to handle numeric postal codes/floats
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
        if not any(field in row and not pd.isna(row[field]) for field in fields.values()):
            if self.debug:
                self.logger.debug(f"No address data found for prefix {prefix}")
            return None
            
        # Log the extracted fields for debugging
        if self.debug:
            self.logger.debug(f"Extracting {prefix} address fields:")
            for key, field in fields.items():
                value = row[field] if field in row else None
                self.logger.debug(f"  {key}: {value}")
            
        return {
            key: self._clean_address_field(row[field] if field in row else None)
            for key, field in fields.items()
        }

    def _process_address(self, session: Session, address_dict: Dict[str, str]) -> Optional[str]:
        """Process a single address and return its ID."""
        if not address_dict:
            if self.debug:
                self.logger.debug("Address dict is empty")
            return None
            
        if not address_dict['line1']:
            if self.debug:
                self.logger.debug(f"Missing line1 in address: {address_dict}")
            return None
            
        # Generate hash for deduplication
        address_hash = self._generate_address_hash(address_dict)
        
        # Check cache first
        if address_hash in self.address_cache:
            self.stats.duplicate_addresses += 1
            return self.address_cache[address_hash]
            
        # Check if address already exists in database
        existing_address = session.query(Address).filter_by(id=address_hash[:32]).first()
        if existing_address:
            self.stats.duplicate_addresses += 1
            return existing_address.id
            
        # Create new address
        address = Address(
            id=address_hash[:32],  # Use first 32 chars of hash as ID
            **address_dict
        )
        
        session.add(address)
        self.address_cache[address_hash] = address.id
        self.stats.unique_addresses += 1
        
        if self.debug:
            self.logger.debug(f"Created new address: {address_dict['line1']}")
        
        return address.id

    def _process_batch(self, session: Session, batch_df: pd.DataFrame) -> pd.DataFrame:
        """Process a batch of addresses.
        
        Args:
            session: Database session for this batch
            batch_df: DataFrame containing batch of rows to process
            
        Returns:
            Processed DataFrame with address IDs
        """
        if self.debug:
            self.logger.debug(f"Processing batch of {len(batch_df)} rows")
            
        # Add columns for address IDs if they don't exist
        batch_df['billing_address_id'] = None
        batch_df['shipping_address_id'] = None
        
        # Process each row in the batch
        for idx, row in batch_df.iterrows():
            try:
                # Extract and process billing address
                billing_dict = self._extract_address(row, 'Billing Address')
                billing_id = self._process_address(session, billing_dict) if billing_dict else None
                if billing_id:
                    self.stats.billing_addresses += 1
                    
                # Extract and process shipping address
                shipping_dict = self._extract_address(row, 'Shipping Address')
                shipping_id = self._process_address(session, shipping_dict) if shipping_dict else None
                if shipping_id:
                    self.stats.shipping_addresses += 1
                    
                # Check if addresses are identical
                if billing_id and shipping_id and billing_id == shipping_id:
                    self.stats.identical_billing_shipping += 1
                    
                # Update the dataframe with address IDs
                batch_df.at[idx, 'billing_address_id'] = billing_id
                batch_df.at[idx, 'shipping_address_id'] = shipping_id
                
            except Exception as e:
                self.logger.error(f"Error processing address: {e}")
                if self.debug:
                    self.logger.debug(f"Error details:", exc_info=True)
                self.stats.total_errors += 1
                continue
        
        return batch_df

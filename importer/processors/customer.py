"""Processor for handling customer data."""
from typing import Dict, Optional
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import select

from ..db.models import Customer, Company, Address
from .base import BaseProcessor

class CustomerProcessor(BaseProcessor):
    """Processes customer data from CSV imports."""
    
    def __init__(self, session: Session):
        """Initialize processor with database session."""
        self.session = session
        self.stats = {
            'customers_processed': 0,
            'customers_created': 0,
            'missing_company_domains': 0,
            'invalid_billing_addresses': 0,
            'invalid_shipping_addresses': 0,
            'errors': 0
        }
        # Cache company domains and address IDs for performance
        self.company_domains = set()
        self.address_ids = set()
        self._load_cached_data()

    def _load_cached_data(self):
        """Load existing company domains and address IDs into cache."""
        # Cache company domains
        companies = self.session.execute(select(Company.domain)).scalars()
        self.company_domains.update(companies)
        
        # Cache address IDs
        addresses = self.session.execute(select(Address.id)).scalars()
        self.address_ids.update(addresses)

    def _verify_company_domain(self, domain: str) -> bool:
        """Verify company domain exists."""
        return domain.lower() in self.company_domains

    def _verify_address_id(self, address_id: Optional[str]) -> bool:
        """Verify address ID exists if provided."""
        return address_id is None or address_id in self.address_ids

    def process_row(self, row: pd.Series) -> Optional[str]:
        """Process a single customer row."""
        self.stats['customers_processed'] += 1
        
        try:
            # Extract required fields
            name = row['Customer Name']
            quickbooks_id = str(row['QuickBooks Internal Id'])
            
            # Extract and verify company domain
            if 'company_domain' not in row or pd.isna(row['company_domain']) or not row['company_domain']:
                # Try to extract domain from email fields
                for field in ['Main Email', 'CC Email', 'Work Email']:
                    if field in row and pd.notna(row[field]) and '@' in str(row[field]):
                        company_domain = str(row[field]).split('@')[1].strip().lower()
                        break
                else:
                    self.stats['missing_company_domains'] += 1
                    return None
            else:
                company_domain = row['company_domain'].lower()
            
            # Verify company domain exists
            if not self._verify_company_domain(company_domain):
                self.stats['missing_company_domains'] += 1
                return None
                
            # Get optional address IDs
            billing_id = row.get('billing_address_id')
            shipping_id = row.get('shipping_address_id')
            
            # Verify address IDs if present
            if billing_id and not self._verify_address_id(billing_id):
                self.stats['invalid_billing_addresses'] += 1
                billing_id = None
                
            if shipping_id and not self._verify_address_id(shipping_id):
                self.stats['invalid_shipping_addresses'] += 1
                shipping_id = None
            
            # Create customer record
            customer = Customer.create(
                name=name,
                quickbooks_id=quickbooks_id,
                company_domain=company_domain,
                billing_address_id=billing_id,
                shipping_address_id=shipping_id
            )
            
            # Verify the customer was created with correct field mappings
            if not all([
                customer.customerName == name,
                customer.quickbooksId == quickbooks_id,
                customer.companyDomain == company_domain.lower(),
                customer.billingAddressId == billing_id,
                customer.shippingAddressId == shipping_id
            ]):
                raise ValueError("Customer field mapping error")
            
            self.session.add(customer)
            self.stats['customers_created'] += 1
            
            return customer.id
            
        except Exception as e:
            self.stats['errors'] += 1
            print(f"Error processing customer: {e}")
            return None

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process all customers in the dataframe."""
        # Add column for customer IDs
        df['customer_id'] = None
        
        # Process each row
        for idx, row in df.iterrows():
            customer_id = self.process_row(row)
            df.at[idx, 'customer_id'] = customer_id
            
        # Commit all customers
        self.session.commit()
        
        return df

    def get_stats(self) -> Dict[str, int]:
        """Get processing statistics."""
        return self.stats

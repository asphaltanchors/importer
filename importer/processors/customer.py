"""Processor for handling customer data."""
from typing import Dict, Optional, Tuple, List, Any
import logging
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import select, func

from ..db.models import Customer, Company, Address
from .base import BaseProcessor
from ..utils.normalization import normalize_customer_name, find_customer_by_name
from ..db.session import SessionManager

class CustomerProcessor(BaseProcessor[Dict[str, Any]]):
    """Processes customer data from CSV imports."""
    
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
            batch_size: Number of records to process in each batch
            error_limit: Maximum number of errors before stopping
            debug: Enable debug logging
        """
        session_manager = SessionManager(config['database_url'])
        super().__init__(session_manager, batch_size, error_limit, debug)
        
        # Initialize customer-specific stats
        self.stats.customers_created = 0
        self.stats.customers_updated = 0
        self.stats.missing_company_domains = 0
        self.stats.invalid_billing_addresses = 0
        self.stats.invalid_shipping_addresses = 0
        # Cache company domains and address IDs for performance
        self.company_domains = set()
        self.address_ids = set()
        with self.session_manager as session:
            self._load_cached_data(session)

    def _load_cached_data(self, session: Session):
        """Load existing company domains and address IDs into cache."""
        self.logger.debug("Loading cached data...")
        
        # Cache company domains
        self.logger.debug("Querying company domains...")
        domains = [d.domain for d in session.query(Company.domain).all()]
        self.logger.debug(f"Raw domain results: {domains}")
        self.company_domains.update(domains)
        self.logger.debug(f"Cached company domains: {self.company_domains}")
        
        # Cache address IDs
        self.logger.debug("Querying address IDs...")
        address_ids = [a.id for a in session.query(Address.id).all()]
        self.address_ids.update(address_ids)
        self.logger.debug(f"Cached {len(self.address_ids)} address IDs")

    def _verify_company_domain(self, domain: str) -> bool:
        """Verify company domain exists."""
        domain = domain.lower()
        exists = domain in self.company_domains
        self.logger.debug(f"Verifying company domain '{domain}': {'exists' if exists else 'not found'}")
        return exists

    def _verify_address_id(self, address_id: Optional[str]) -> bool:
        """Verify address ID exists if provided."""
        return address_id is None or address_id in self.address_ids

    def _find_customer_by_name(self, session: Session, name: str) -> Tuple[Optional[Customer], bool]:
        """Find a customer by name using the shared matching logic."""
        return find_customer_by_name(session, name)

    def _process_batch(self, session: Session, batch_df: pd.DataFrame) -> pd.DataFrame:
        """Process a batch of customer rows."""
        batch_df['customer_id'] = None
        
        for idx, row in batch_df.iterrows():
            try:
                # Try both possible column names
                customer_name = None
                for column in ['Customer Name', 'Customer']:
                    if column in row and not pd.isna(row[column]):
                        customer_name = row[column]
                        if column == 'Customer':
                            self.logger.debug(f"Using 'Customer' column instead of 'Customer Name'")
                        break

                if customer_name is None:
                    self.logger.warning(f"Skipping row with missing or NaN customer name, QuickBooks ID: {row.get('QuickBooks Internal Id', 'unknown')}")
                    continue

                name = str(customer_name)  # Convert to string after NaN check
                    
                quickbooks_id = str(row['QuickBooks Internal Id'])
                
                # Extract and verify company domain
                if 'company_domain' not in row or pd.isna(row['company_domain']) or not row['company_domain']:
                    # Try to extract domain from email fields
                    for field in ['Main Email', 'CC Email', 'Work Email']:
                        if field in row and pd.notna(row[field]) and '@' in str(row[field]):
                            company_domain = str(row[field]).split('@')[1].strip().lower()
                            break
                    else:
                        self.stats.missing_company_domains += 1
                        continue
                else:
                    company_domain = row['company_domain'].lower()
                
                # Verify company domain exists
                if not self._verify_company_domain(company_domain):
                    self.stats.missing_company_domains += 1
                    continue
                
                # Get optional address IDs
                billing_id = row.get('billing_address_id')
                shipping_id = row.get('shipping_address_id')
                
                # Verify address IDs if present
                if billing_id and not self._verify_address_id(billing_id):
                    self.stats.invalid_billing_addresses += 1
                    billing_id = None
                    
                if shipping_id and not self._verify_address_id(shipping_id):
                    self.stats.invalid_shipping_addresses += 1
                    shipping_id = None
                
                # First try to find by QuickBooks ID if it exists and is valid
                if not pd.isna(quickbooks_id) and quickbooks_id.strip():
                    self.logger.debug(f"Searching for customer by QuickBooks ID: {quickbooks_id}")
                    # Try finding by both id and quickbooksId since we now use QB ID as primary key
                    existing_customer = (
                        session.query(Customer)
                        .filter(
                            (Customer.id == quickbooks_id) | 
                            (Customer.quickbooksId == quickbooks_id)
                        )
                        .first()
                    )
                    if existing_customer:
                        self.logger.debug(f"Found existing customer by QuickBooks ID: {existing_customer.customerName}")
                else:
                    existing_customer = None
                    quickbooks_id = None  # Ensure it's None if empty/invalid
                
                # If not found by ID, try name matching (only if name is valid)
                if not existing_customer and not pd.isna(name):
                    self.logger.debug(f"Searching for customer by name: {name}")
                    existing_customer, used_normalization = self._find_customer_by_name(session, name)
                    if existing_customer:
                        if used_normalization:
                            self.stats.normalized_matches += 1
                            self.logger.info(f"Updating existing customer found by normalized name match: '{name}' -> '{existing_customer.customerName}'")
                        else:
                            self.logger.debug(f"Found existing customer by exact name match: {existing_customer.customerName}")
                
                if existing_customer:
                    # Update existing customer
                    self.logger.debug(f"Updating existing customer: {name}")
                    self.logger.debug(f"Company domain: {company_domain}")
                    self.logger.debug(f"Billing address ID: {billing_id}")
                    self.logger.debug(f"Shipping address ID: {shipping_id}")
                    
                    existing_customer.customerName = name
                    existing_customer.companyDomain = company_domain.lower()
                    existing_customer.billingAddressId = billing_id
                    existing_customer.shippingAddressId = shipping_id
                    existing_customer.modifiedAt = pd.Timestamp.now()
                    customer = existing_customer
                    self.stats.customers_updated += 1
                else:
                    # Create new customer
                    self.logger.debug(f"Creating new customer: {name}")
                    self.logger.debug(f"Company domain: {company_domain}")
                    self.logger.debug(f"Billing address ID: {billing_id}")
                    self.logger.debug(f"Shipping address ID: {shipping_id}")
                    
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
                    
                    session.add(customer)
                    self.stats.customers_created += 1
                
                batch_df.at[idx, 'customer_id'] = customer.id
                
            except Exception as e:
                self.logger.error(f"Error processing customer: {e}")
                continue
        
        return batch_df

    def validate_data(self, df: pd.DataFrame) -> Tuple[List[str], List[str]]:
        """Validate customer data before processing.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (critical_issues, warnings)
        """
        critical_issues = []
        warnings = []
        
        # Check for required columns
        required_columns = ['Customer Name', 'QuickBooks Internal Id']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            critical_issues.append(f"Missing required columns: {', '.join(missing_columns)}")
        
        # Check for empty customer names
        if 'Customer Name' in df.columns:
            empty_names = df['Customer Name'].isna().sum()
            if empty_names > 0:
                warnings.append(f"Found {empty_names} rows with empty customer names")
        
        return critical_issues, warnings

    def get_stats(self) -> Dict[str, Any]:
        """Get processing statistics."""
        return self.stats.to_dict()

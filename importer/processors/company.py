"""Company processor for handling email domain extraction and company creation."""
from typing import Dict, Any, List, Set, Optional, Tuple
import pandas as pd
from sqlalchemy.orm import Session

from ..db.models import Company
from ..db.session import SessionManager
from .base import BaseProcessor
from ..utils.normalization import normalize_domain

class CompanyProcessor(BaseProcessor[Dict[str, Any]]):
    """Processes company-related data from customer records."""
    
    # Fields that may contain email addresses
    EMAIL_FIELDS = [
        'Main Email',
        'CC Email',
        'Work Email',
        'Notes',  # Sometimes contains email addresses
        'Additional Notes'
    ]

    def __init__(
        self,
        config: Dict[str, Any],
        batch_size: int = 100,
        error_limit: int = 1000,
        debug: bool = False
    ):
        """Initialize the company processor.
        
        Args:
            config: Configuration dictionary containing database_url
            batch_size: Number of records to process per batch
            error_limit: Maximum number of errors before stopping
            debug: Enable debug logging
        """
        session_manager = SessionManager(config['database_url'])
        super().__init__(session_manager, batch_size, error_limit, debug)
        self.processed_domains: Set[str] = set()
        
        # Add company-specific stats
        self.stats.domains_extracted = 0
        self.stats.companies_created = 0
        self.stats.rows_with_domain = 0

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
        required_columns = ['Customer']  # At minimum need customer name to extract domain
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            critical_issues.append(f"Missing required columns: {', '.join(missing_columns)}")
            return critical_issues, warnings
        
        # Check for empty customer names
        empty_customers = df[df['Customer'].isna()]
        if not empty_customers.empty:
            msg = (f"Found {len(empty_customers)} rows with missing customer names that will be skipped. "
                  f"First few row numbers: {', '.join(map(str, empty_customers.index[:3]))}")
            warnings.append(msg)
        
        # Check for potential data quality issues
        if not any(field in df.columns for field in self.EMAIL_FIELDS):
            warnings.append("No email fields found in data - domain extraction may be limited")
        
        return critical_issues, warnings

    def extract_email_domain(self, row: pd.Series) -> str:
        """Extract domain from row data.
        
        Tries the following in order:
        1. Billing email domain if present
        2. Email fields for domain extraction
        3. Phone fields that might contain misplaced emails
        
        Args:
            row: DataFrame row containing customer data
            
        Returns:
            Extracted and normalized domain, or empty string if none found
        """
        # First check if we have a billing email domain
        if 'Billing Address Email' in row and pd.notna(row['Billing Address Email']):
            email = str(row['Billing Address Email']).strip()
            if '@' in email:
                try:
                    raw_domain = email.split('@')[1].strip()
                    domain = normalize_domain(raw_domain)
                    if domain:
                        return domain
                except IndexError:
                    pass

        # Then check email fields
        for field in self.EMAIL_FIELDS:
            if field not in row:
                continue
                
            value = str(row[field]).strip()
            if not value or value.lower() == 'nan':
                continue

            # Split on common separators and look for emails
            potential_emails = [
                e.strip() 
                for e in value.replace(';', ',').split(',')
            ]
            
            for email in potential_emails:
                if '@' in email:
                    try:
                        raw_domain = email.split('@')[1].strip()
                        domain = normalize_domain(raw_domain)
                        if domain:  # normalize_domain returns None for invalid domains
                            return domain
                    except IndexError:
                        continue

        # Finally check other fields that might contain misplaced emails
        for field in ['Main Phone', 'Alt. Phone', 'Work Phone', 'Mobile', 'Fax']:
            if field not in row:
                continue
                
            value = str(row[field]).strip()
            if '@' in value:
                try:
                    raw_domain = value.split('@')[1].strip()
                    domain = normalize_domain(raw_domain)
                    if domain:
                        return domain
                except IndexError:
                    continue

        # If no email domain found, try to extract from company name
        if 'Customer' in row and pd.notna(row['Customer']):
            company_name = str(row['Customer']).strip()
            # Try to find domain-like parts in the company name
            parts = company_name.lower().replace(',', ' ').replace('(', ' ').replace(')', ' ').split()
            for part in parts:
                # Add protocol to help tld module parse correctly
                potential_domain = f"http://{part}"
                domain = normalize_domain(potential_domain)
                if domain:
                    if self.debug:
                        self.logger.debug(f"Extracted domain '{domain}' from company name part '{part}'")
                    return domain
                
            if self.debug:
                self.logger.debug(f"No valid domain found in company name: {company_name}")

        return ''

    def _process_batch(self, session: Session, batch_df: pd.DataFrame) -> pd.DataFrame:
        """Process a batch of customer records.
        
        Args:
            session: Database session for this batch
            batch_df: DataFrame containing the batch data
            
        Returns:
            Processed DataFrame with company_domain column added
        """
        if self.debug:
            self.logger.debug(f"Processing batch of {len(batch_df)} records")
            
        # Extract domains for each row
        batch_df['company_domain'] = batch_df.apply(self.extract_email_domain, axis=1)
        
        # Get unique domains and their QuickBooks IDs
        domain_qb_map = {}
        for _, row in batch_df.iterrows():
            domain = row['company_domain']
            if domain and domain not in domain_qb_map:
                qb_id = str(row.get('QuickBooks Internal Id', '')) if pd.notna(row.get('QuickBooks Internal Id')) else None
                domain_qb_map[domain] = qb_id
        
        valid_domains = [d for d in domain_qb_map.keys() if d]
        
        # Update statistics
        self.stats.domains_extracted += len(valid_domains)
        self.stats.rows_with_domain += len(batch_df[batch_df['company_domain'].str.len() > 0])
        
        # Create companies for new domains
        for domain in valid_domains:
            if not domain:  # Skip empty domains
                continue
                
            # Check if company exists
            existing = session.query(Company).filter(
                Company.domain == domain
            ).first()
            
            if not existing:
                company = Company.create_from_domain(domain, quickbooks_id=domain_qb_map[domain])
                session.add(company)
                self.stats.companies_created += 1
                
                if self.debug:
                    self.logger.debug(f"Created new company for domain: {domain}")
        
        # Track processed domains
        self.processed_domains.update(valid_domains)
        
        return batch_df

    def get_processed_domains(self) -> Set[str]:
        """Get the set of processed domains."""
        return self.processed_domains.copy()  # Return copy to prevent modification

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
        'Billing Address Email',
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
        self.stats.validation_errors = 0

    def validate_data(self, df: pd.DataFrame) -> Tuple[List[str], List[str]]:
        """Validate data before processing.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (critical_issues, warnings)
        """
        critical_issues = []
        warnings = []
        
        # Check for required columns - either Customer or Customer Name is required
        if 'Customer' not in df.columns and 'Customer Name' not in df.columns:
            critical_issues.append("Missing required columns: Customer or Customer Name")
            self.stats.total_errors += 1
            return critical_issues, warnings
        
        # Use the available column name
        customer_column = 'Customer' if 'Customer' in df.columns else 'Customer Name'
        
        # Check for empty or invalid customer names
        empty_customers = df[df[customer_column].isna() | (df[customer_column].astype(str).str.strip() == '')]
        if not empty_customers.empty:
            msg = (f"Found {len(empty_customers)} rows with missing or empty customer names that will be skipped. "
                  f"First few row numbers: {', '.join(map(str, empty_customers.index[:3]))}")
            warnings.append(msg)  # Keep as warning to match test expectations
            self.stats.validation_errors += len(empty_customers)
            self.stats.total_errors += len(empty_customers)
            
            # If we exceed error limit, mark batch as failed
            if self.stats.total_errors >= self.error_limit:
                self.stats.failed_batches += 1
        
        # Check for potential data quality issues
        if not any(field in df.columns for field in self.EMAIL_FIELDS):
            warnings.append("No email fields found in data - domain extraction may be limited")
        
        return critical_issues, warnings

    def extract_email_domain(self, row: pd.Series) -> str:
        """Extract domain from row data.
        
        Tries the following in order:
        1. Main Email domain if present
        2. Other email fields for domain extraction
        3. Company name if it looks like a domain
        
        Args:
            row: DataFrame row containing customer data
            
        Returns:
            Extracted and normalized domain, or empty string if none found
        """
        # First check Main Email and Billing Address Email as primary sources
        primary_fields = ['Main Email', 'Billing Address Email']
        for field in primary_fields:
            if field in row and pd.notna(row[field]):
                # Split multiple emails
                emails = str(row[field]).strip().split(';')
                for email in emails:
                    email = email.strip()
                    if '@' in email:
                        try:
                            raw_domain = email.split('@')[1].strip()
                            if self.debug:
                                self.logger.debug(f"Attempting to normalize domain: {raw_domain}")
                            domain = normalize_domain(raw_domain)
                            if domain:
                                if self.debug:
                                    self.logger.debug(f"Successfully normalized domain: {domain}")
                                return domain
                            elif self.debug:
                                self.logger.debug(f"normalize_domain returned None for: {raw_domain}")
                        except IndexError:
                            if self.debug:
                                self.logger.debug(f"IndexError extracting domain from: {email}")
                            continue

        # Then check other email fields
        for field in self.EMAIL_FIELDS:
            if field not in row or field in ['Main Email', 'Billing Address Email']:  # Skip already checked fields
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
                        return ''  # Return empty string when normalize_domain returns None
                    except IndexError:
                        continue

        # If no email domain found, try to extract from company name
        customer_column = 'Customer' if 'Customer' in row else 'Customer Name'
        if customer_column in row and pd.notna(row[customer_column]):
            company_name = str(row[customer_column]).strip().lower()
            if '.' in company_name:  # Only try company names that look like domains
                domain = normalize_domain(company_name)
                if domain:
                    if self.debug:
                        self.logger.debug(f"Extracted domain '{domain}' from company name: {company_name}")
                    return domain
                return ''  # Return empty string when normalize_domain returns None

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
            
        if self.debug:
            self.logger.debug("Starting domain extraction and normalization")
            
        # Extract domains for each row (normalize_domain is already called within extract_email_domain)
        batch_df['company_domain'] = batch_df.apply(self.extract_email_domain, axis=1)
        
        if self.debug:
            self.logger.debug(f"Extracted domains: {batch_df['company_domain'].tolist()}")
        
        # Get unique domains and their QuickBooks IDs
        domain_qb_map = {}
        for _, row in batch_df.iterrows():
            domain = row['company_domain']
            if domain and domain not in domain_qb_map:
                qb_id = str(row.get('QuickBooks Internal Id', '')) if pd.notna(row.get('QuickBooks Internal Id')) else None
                domain_qb_map[domain] = qb_id
                if self.debug:
                    self.logger.debug(f"Added domain to map: {domain} (QB ID: {qb_id})")
        
        valid_domains = list(domain_qb_map.keys())
        if self.debug:
            self.logger.debug(f"Valid domains for processing: {valid_domains}")
        
        # Update statistics
        self.stats.domains_extracted += len(valid_domains)
        self.stats.rows_with_domain += len(batch_df[batch_df['company_domain'].str.len() > 0])
        
        # Process each domain in the batch, skipping rows with validation errors
        successful_domains = []
        for domain in valid_domains:
            if not domain:  # Skip empty domains
                continue
                
            try:
                if self.debug:
                    self.logger.debug(f"\nProcessing domain: {domain}")
                    
                existing = session.query(Company).filter(
                    Company.domain == domain  # Domain is already normalized from extract_email_domain
                ).first()
                
                if existing:
                    if self.debug:
                        self.logger.debug(f"Found existing company for domain: {domain}")
                    continue
                    
                if self.debug:
                    self.logger.debug(f"Creating new company for domain: {domain}")
                    
                company = Company.create_from_domain(domain, quickbooks_id=domain_qb_map[domain])
                session.add(company)
                try:
                    session.flush()  # Check for constraint violations before commit
                    successful_domains.append(domain)
                    self.stats.companies_created += 1
                    if self.debug:
                        self.logger.debug(f"Successfully created company for domain: {domain}")
                except Exception as e:
                    session.rollback()
                    self.stats.total_errors += 1
                    if self.debug:
                        self.logger.error(f"Database error creating company for domain {domain}: {str(e)}")
                    continue
            except Exception as e:
                self.stats.total_errors += 1
                if self.debug:
                    self.logger.error(f"Error processing domain {domain}: {str(e)}")
                continue

        try:
            if successful_domains:
                session.commit()  # Commit successful companies
                if self.debug:
                    self.logger.debug(f"Successfully created companies for domains: {successful_domains}")
        except Exception as e:
            session.rollback()
            self.stats.total_errors += 1
            if self.debug:
                self.logger.error(f"Error committing batch: {str(e)}")
        
        
        # Track processed domains (already normalized)
        self.processed_domains.update(valid_domains)
        
        return batch_df

    def get_processed_domains(self) -> Set[str]:
        """Get the set of processed domains."""
        return self.processed_domains.copy()  # Return copy to prevent modification

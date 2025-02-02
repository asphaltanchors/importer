"""Company processor for handling email domain extraction and company creation."""
from typing import Dict, Any, List, Set
import pandas as pd
from sqlalchemy.orm import Session
from ..db.models import Company
from ..db.session import SessionManager
from .base import BaseProcessor

class CompanyProcessor(BaseProcessor):
    """Processes company-related data from customer records."""
    
    # Fields that may contain email addresses
    EMAIL_FIELDS = [
        'Main Email',
        'CC Email',
        'Work Email',
        'Notes',  # Sometimes contains email addresses
        'Additional Notes'
    ]

    def __init__(self, config: Dict[str, Any], batch_size: int = 100):
        """Initialize the company processor."""
        super().__init__(None, batch_size)  # Initialize without session
        self.session_manager = SessionManager(config['database_url'])
        self.processed_domains: Set[str] = set()
        self.stats.update({
            'domains_extracted': 0,
            'companies_created': 0,
            'rows_with_domain': 0
        })
        
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """Process the data in batches with session management."""
        with self.session_manager as session:
            self.session = session  # Set session for batch processing
            result = super().process(data)  # Use parent's batch processing
            self.session = None  # Clear session after processing
            return result

    def extract_email_domain(self, row: pd.Series) -> str:
        """Extract the first valid email domain from a row's email fields."""
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
                        domain = email.split('@')[1].strip()
                        if '.' in domain:  # Basic domain validation
                            return domain.lower()
                    except IndexError:
                        continue

        # Check other fields that might contain misplaced emails
        for field in ['Main Phone', 'Alt. Phone', 'Work Phone', 'Mobile', 'Fax']:
            if field not in row:
                continue
                
            value = str(row[field]).strip()
            if '@' in value:
                try:
                    domain = value.split('@')[1].strip()
                    if '.' in domain:
                        return domain.lower()
                except IndexError:
                    continue

        return ''

    def _process_batch(self, batch_df: pd.DataFrame) -> pd.DataFrame:
        """Process a batch of customer records."""
        # Extract domains for each row
        batch_df['company_domain'] = batch_df.apply(self.extract_email_domain, axis=1)
        
        # Get unique domains from this batch
        domains = batch_df['company_domain'].unique()
        valid_domains = [d for d in domains if d]
        
        # Update statistics
        self.stats['domains_extracted'] += len(valid_domains)
        self.stats['rows_with_domain'] += len(batch_df[batch_df['company_domain'].str.len() > 0])
        
        # Create companies for new domains
        for domain in valid_domains:
            if not domain:  # Skip empty domains
                continue
                
            # Check if company exists
            existing = self.session.query(Company).filter(
                Company.domain == domain
            ).first()
            
            if not existing:
                company = Company.create_from_domain(domain)
                self.session.add(company)
                self.stats['companies_created'] += 1
        
        # Track processed domains
        self.processed_domains.update(valid_domains)
        
        return batch_df

    def get_stats(self) -> Dict[str, int]:
        """Get processing statistics."""
        return self.stats

    def get_processed_domains(self) -> Set[str]:
        """Get the set of processed domains."""
        return self.processed_domains

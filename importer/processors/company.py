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

    def __init__(self, config: Dict[str, Any]):
        """Initialize the company processor."""
        super().__init__(config)
        self.processed_domains: Set[str] = set()
        self.stats = {
            'domains_extracted': 0,
            'companies_created': 0,
            'rows_processed': 0,
            'rows_with_domain': 0
        }
        self.session_manager = SessionManager(config['database_url'])

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

    def create_companies(self, session: Session, domains: List[str]) -> None:
        """Create company records for new domains."""
        for domain in domains:
            if not domain:  # Skip empty domains
                continue
                
            # Check if company exists
            existing = session.query(Company).filter(
                Company.domain == domain
            ).first()
            
            if not existing:
                company = Company.create_from_domain(domain)
                session.add(company)
                self.stats['companies_created'] += 1

    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """Process the customer data to extract and create company records."""
        self.stats['rows_processed'] = len(data)
        
        # Extract domains for each row
        data['company_domain'] = data.apply(self.extract_email_domain, axis=1)
        
        # Update statistics
        domains = data['company_domain'].unique()
        valid_domains = [d for d in domains if d]
        
        self.stats['domains_extracted'] = len(valid_domains)
        self.stats['rows_with_domain'] = len(data[data['company_domain'].str.len() > 0])
        
        # Create company records
        with self.session_manager as session:
            self.create_companies(session, valid_domains)
        
        # Track processed domains
        self.processed_domains.update(valid_domains)
        
        return data

    def get_stats(self) -> Dict[str, int]:
        """Get processing statistics."""
        return self.stats

    def get_processed_domains(self) -> Set[str]:
        """Get the set of processed domains."""
        return self.processed_domains

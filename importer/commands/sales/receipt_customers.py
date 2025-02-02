"""Process customers from sales receipts command."""

from pathlib import Path
import click
import pandas as pd
from typing import Optional, Dict, Any
from datetime import datetime
import json

from ...cli.base import FileInputCommand, command_error_handler
from ...cli.logging import get_logger
from ...utils.normalization import normalize_customer_name, find_customer_by_name
from ...utils.csv_normalization import normalize_dataframe_columns
from ...db.models import Customer, Company
from ...db.session import SessionManager

class ProcessReceiptCustomersCommand(FileInputCommand):
    """Command to process customers from sales receipt data."""
    
    def __init__(self, config, input_file: Path, output_file: Optional[Path] = None, batch_size: int = 50, error_limit: int = 1000):
        """Initialize the command.
        
        Args:
            config: Application configuration
            input_file: Path to input CSV file
            output_file: Optional path to save results
            batch_size: Number of records to process per batch
            error_limit: Maximum number of errors before stopping
        """
        super().__init__(config, input_file, output_file)
        self.batch_size = batch_size
        self.error_limit = error_limit
        self.stats = {
            'total_processed': 0,
            'customers_found': 0,
            'customers_normalized': 0,
            'customers_created': 0,
            'amazon_fba_created': 0,
            'errors': 0
        }
        # Track processed QuickBooks IDs to avoid duplicates
        self.processed_qb_ids = set()
    
    def validate_data(self, df: pd.DataFrame) -> tuple[list[str], list[str]]:
        """Validate the input data.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Tuple of (critical_issues, warnings)
        """
        critical_issues = []
        warnings = []
        
        # Check for required Customer column
        if 'Customer' not in df.columns:
            critical_issues.append("Missing required 'Customer' column")
            return critical_issues, warnings
            
        # Check for empty customer names
        empty_customers = df[df['Customer'].isna()]
        if not empty_customers.empty:
            warnings.append(f"Found {len(empty_customers)} rows with missing customer names that will be skipped")
            
        return critical_issues, warnings
    
    def _process_amazon_fba(self, session, customer_name: str, city: str, row: pd.Series) -> Customer:
        """Process Amazon FBA customer with special handling.
        
        Args:
            session: Database session
            customer_name: Customer name (should be 'Amazon FBA')
            city: City from the address
            
        Returns:
            Customer object
        """
        if not city:
            raise ValueError("City is required for Amazon FBA customers")
            
        # Create unique name with city
        full_name = f"Amazon FBA - {city}"
        
        # First try to find by QuickBooks ID
        quickbooks_id = row.get('QuickBooks Internal Id', '')
        if not pd.isna(quickbooks_id):
            quickbooks_id = str(quickbooks_id).strip()
            customer = session.query(Customer).filter(
                Customer.quickbooksId == quickbooks_id
            ).first()
            if customer:
                # Update name if needed
                if customer.customerName != full_name:
                    customer.customerName = full_name
                    customer.modifiedAt = datetime.utcnow()
                return customer
        else:
            quickbooks_id = None

        # Then try by name
        customer = session.query(Customer).filter(
            Customer.customerName == full_name
        ).first()
        
        if customer:
            # Update QuickBooks ID if we have one and customer doesn't
            if quickbooks_id and not customer.quickbooksId:
                customer.quickbooksId = quickbooks_id
                customer.modifiedAt = datetime.utcnow()
        else:
            # Create new Amazon FBA customer
            customer = Customer.create(
                name=full_name,
                quickbooks_id=quickbooks_id,
                company_domain="amazon-fba.com",  # Special domain for Amazon FBA
                billing_address_id=None,
                shipping_address_id=None
            )
            session.add(customer)
            self.stats['amazon_fba_created'] += 1
            if self.debug:
                self.logger.debug(f"Created new Amazon FBA customer: {full_name}")
                
        return customer
    
    def _create_customer(self, session, customer_name: str, row: pd.Series, email: Optional[str] = None) -> Customer:
        """Create a new customer record.
        
        Args:
            session: Database session
            customer_name: Customer name
            email: Optional email address
            
        Returns:
            Customer object
        """
        # First try to find by name
        customer = session.query(Customer).filter(
            Customer.customerName == customer_name
        ).first()
        
        if customer:
            # Update QuickBooks ID if we have one and customer doesn't
            quickbooks_id = row.get('QuickBooks Internal Id', '')
            if not pd.isna(quickbooks_id):
                quickbooks_id = str(quickbooks_id).strip()
                if not customer.quickbooksId:
                    customer.quickbooksId = quickbooks_id
                    customer.modifiedAt = datetime.utcnow()
            return customer
            
        # Extract domain from email or use default
        if email and '@' in email:
            domain = email.split('@')[1].lower()
        else:
            domain = "unknown-domain.com"
            
        quickbooks_id = row.get('QuickBooks Internal Id', '')
        if pd.isna(quickbooks_id):
            quickbooks_id = None
        else:
            quickbooks_id = str(quickbooks_id).strip()
            
        # Create new customer
        customer = Customer.create(
            name=customer_name,
            quickbooks_id=quickbooks_id,
            company_domain=domain,
            billing_address_id=None,
            shipping_address_id=None
        )
        session.add(customer)
        self.stats['customers_created'] += 1
        if self.debug:
            self.logger.debug(f"Created new customer: {customer_name} with domain {domain}")
            
        return customer
    
    @command_error_handler
    def execute(self) -> None:
        """Execute the command."""
        self.logger.info(f"Processing customers from {self.input_file}")
        self.logger.info(f"Batch size: {self.batch_size}, Error limit: {self.error_limit}")
        
        # Read CSV file
        self.logger.info("Reading CSV file...")
        if self.debug:
            self.logger.debug(f"Reading CSV with low_memory=False from {self.input_file}")
        df = pd.read_csv(self.input_file, low_memory=False)
        if self.debug:
            self.logger.debug("Normalizing dataframe columns")
        df = normalize_dataframe_columns(df)
        self.logger.info(f"Found {len(df)} rows")
        
        result = self.process(df)
        
        # Save results if output file specified
        if self.output_file:
            if self.debug:
                self.logger.debug(f"Saving results to {self.output_file}")
            with open(self.output_file, 'w') as f:
                json.dump(result, f, indent=2)
            self.logger.info(f"Results saved to {self.output_file}")
    
    def _ensure_required_companies(self, session) -> None:
        """Ensure required companies exist in database.
        
        Args:
            session: Database session
        """
        required_domains = ['amazon-fba.com', 'unknown-domain.com']
        for domain in required_domains:
            company = session.query(Company).filter(
                Company.domain == domain
            ).first()
            if not company:
                company = Company.create_from_domain(domain)
                session.add(company)
                if self.debug:
                    self.logger.debug(f"Created required company: {company}")
        session.commit()

    def process(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Process customers from the sales receipt data.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Dictionary with processing results
        """
        session_manager = SessionManager(self.config.database_url)
        
        try:
            self.logger.info("Processing customers from sales receipts")
            
            # Ensure required companies exist
            with session_manager as session:
                self._ensure_required_companies(session)
            
            # Validate data
            critical_issues, warnings = self.validate_data(df)
            
            if critical_issues:
                self.logger.error("Critical validation issues found:")
                for issue in critical_issues:
                    self.logger.error(f"  - {issue}")
                return {
                    'success': False,
                    'summary': {
                        'validation_issues': critical_issues,
                        'stats': self.stats
                    }
                }
                
            if warnings:
                self.logger.warning("Validation warnings:")
                for warning in warnings:
                    self.logger.warning(f"  - {warning}")
                    
            # Process in batches
            with session_manager as session:
                for start_idx in range(0, len(df), self.batch_size):
                    if self.stats['errors'] >= self.error_limit:
                        self.logger.error(f"Error limit ({self.error_limit}) reached")
                        break
                        
                    batch_df = df.iloc[start_idx:start_idx + self.batch_size]
                    
                    for _, row in batch_df.iterrows():
                        try:
                            customer_name = row.get('Customer')
                            if pd.isna(customer_name):
                                continue
                                
                            customer_name = str(customer_name).strip()
                            
                            # First try to find by QuickBooks ID
                            quickbooks_id = row.get('QuickBooks Internal Id', '')
                            if not pd.isna(quickbooks_id):
                                quickbooks_id = str(quickbooks_id).strip()
                                # Skip if we've already processed this QuickBooks ID
                                if quickbooks_id in self.processed_qb_ids:
                                    continue
                                # Try to find existing customer by QuickBooks ID
                                customer = session.query(Customer).filter(
                                    Customer.quickbooksId == quickbooks_id
                                ).first()
                                if customer:
                                    # Update customer info if needed
                                    if customer.customerName != customer_name:
                                        customer.customerName = customer_name
                                        customer.modifiedAt = datetime.utcnow()
                                    self.stats['customers_found'] += 1
                                    self.processed_qb_ids.add(quickbooks_id)
                                    continue
                            else:
                                quickbooks_id = None
                                
                            # Special handling for Amazon FBA
                            if customer_name.upper() == 'AMAZON FBA':
                                # Extract city from billing address
                                city = row.get('Billing Address City', '')
                                if not pd.isna(city):
                                    customer = self._process_amazon_fba(session, customer_name, str(city).strip(), row)
                                else:
                                    self.logger.error(f"Missing city for Amazon FBA customer")
                                    self.stats['errors'] += 1
                                    continue
                            else:
                                # Only try name matching if no QuickBooks ID
                                if not quickbooks_id:
                                    customer, used_normalization = find_customer_by_name(session, customer_name)
                                    if customer:
                                        if used_normalization:
                                            self.stats['customers_normalized'] += 1
                                        self.stats['customers_found'] += 1
                                        continue
                                
                                # Create new customer
                                email = row.get('Email', '')  # Add email column if available
                                if not pd.isna(email):
                                    email = str(email).strip()
                                else:
                                    email = None
                                customer = self._create_customer(session, customer_name, row, email)
                            
                            self.stats['total_processed'] += 1
                            if quickbooks_id:
                                self.processed_qb_ids.add(quickbooks_id)
                            
                        except Exception as e:
                            self.logger.error(f"Error processing customer '{customer_name}': {str(e)}")
                            self.stats['errors'] += 1
                            if self.debug:
                                self.logger.debug("Error details:", exc_info=True)
                            continue
                            
                    # Commit batch
                    session.commit()
                    
            success = (
                self.stats['errors'] < self.error_limit and
                self.stats['total_processed'] > 0
            )
            
            return {
                'success': success,
                'summary': {
                    'stats': self.stats
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to process customers: {str(e)}")
            if self.debug:
                self.logger.debug("Error details:", exc_info=True)
            return {
                'success': False,
                'summary': {
                    'error': str(e),
                    'stats': self.stats
                }
            }

# Click command wrapper
@click.command()
@click.argument('file_path', type=click.Path(exists=True, path_type=Path))
@click.option('--output', type=click.Path(file_okay=True, dir_okay=False, path_type=Path), help='Save processing results to file')
@click.option('--batch-size', default=50, help='Number of records to process per batch')
@click.option('--error-limit', default=1000, help='Maximum number of errors before stopping')
@click.pass_context
def process_receipt_customers(ctx, file_path: Path, output: Optional[Path], batch_size: int, error_limit: int):
    """Process customers from a sales receipt CSV file."""
    config = ctx.obj.get('config')
    if not config:
        click.echo("Error: No configuration found in context")
        return
        
    command = ProcessReceiptCustomersCommand(config, file_path, output, batch_size, error_limit)
    command.execute()

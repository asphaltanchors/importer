"""
Customer-related commands for the importer CLI.
Handles customer data processing, validation, and management.
"""

import json
import click
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any

from ...cli.base import FileInputCommand, command_error_handler
from ...cli.config import Config
from ...processors.company import CompanyProcessor
from ...processors.address import AddressProcessor
from ...processors.customer import CustomerProcessor
from ...processors.email import EmailProcessor
from ...processors.phone import PhoneProcessor
from ...processors.verifier import ImportVerifier
from sqlalchemy import create_engine, text

class ListCompaniesCommand(FileInputCommand):
    """Command to list most recent companies in the database."""
    
    def __init__(self, config: Config, limit: int = 10):
        super().__init__(config, Path("."))  # Dummy path since we don't use files
        self.limit = limit
    
    @command_error_handler
    def execute(self) -> None:
        """Execute the command."""
        session = self.get_session()
        with session:
            # Get total count
            count_result = session.execute(text('SELECT COUNT(*) FROM "Company"'))
            total_count = count_result.scalar()
            
            # Get limited companies
            companies_result = session.execute(text("""
                SELECT domain, name, "createdAt" 
                FROM "Company" 
                ORDER BY "createdAt" DESC
                LIMIT :limit
            """), {"limit": self.limit})
            companies = companies_result.fetchall()
            
        if not companies:
            click.echo("No companies found in database")
            return
            
        click.echo(f"\nMost recent {self.limit} of {total_count} companies in database:")
        for company in companies:
            click.echo(f"  - {company.domain} ({company.name})")

class ExtractDomainsCommand(FileInputCommand):
    """Command to extract and analyze email domains."""
    
    @command_error_handler
    def execute(self) -> None:
        """Execute the command."""
        self.logger.info(f"Processing {self.input_file} for email domains...")
        
        # Read the CSV file with Windows encoding
        df = pd.read_csv(
            self.input_file,
            encoding='cp1252',
            dtype=str,  # Read all columns as strings to preserve IDs
            skipinitialspace=True
        )
        
        # Initialize and run company processor
        processor = CompanyProcessor({'database_url': self.config.database_url})
        processed_df = processor.process(df)
        
        # Get statistics
        stats = processor.get_stats()
        domains = processor.get_processed_domains()
        
        # Display summary
        click.echo("\nDomain Extraction Summary:")
        click.echo(f"Total Rows Processed: {stats['rows_processed']}")
        click.echo(f"Rows With Domain: {stats['rows_with_domain']}")
        click.echo(f"Unique Domains Found: {stats['domains_extracted']}")
        
        if domains:
            click.echo("\nExtracted Domains:")
            for domain in sorted(domains):
                click.echo(f"  - {domain}")
        
        # Save detailed results if requested
        if self.output_file:
            results = {
                'stats': stats,
                'domains': list(domains),
                'rows_with_domain': len(processed_df[processed_df['company_domain'].str.len() > 0])
            }
            with open(self.output_file, 'w') as f:
                json.dump(results, f, indent=2)
            click.echo(f"\nDetailed results saved to {self.output_file}")

class ProcessAddressesCommand(FileInputCommand):
    """Command to process and deduplicate addresses."""
    
    @command_error_handler
    def execute(self) -> None:
        """Execute the command."""
        self.logger.info(f"Processing {self.input_file} for addresses...")
        
        # Read the CSV file with Windows encoding
        df = pd.read_csv(
            self.input_file,
            encoding='cp1252',
            dtype=str,  # Read all columns as strings to preserve IDs
            skipinitialspace=True
        )
        
        # Initialize and run address processor
        session = self.get_session()
        try:
            processor = AddressProcessor(session)
            processed_df = processor.process(df)
            
            # Get statistics
            stats = processor.get_stats()
            
            # Display summary
            click.echo("\nAddress Processing Summary:")
            click.echo(f"Total Rows Processed: {stats['addresses_processed']}")
            click.echo(f"Unique Addresses Created: {stats['unique_addresses']}")
            click.echo(f"Duplicate Addresses Found: {stats['duplicate_addresses']}")
            click.echo(f"Billing Addresses: {stats['billing_addresses']}")
            click.echo(f"Shipping Addresses: {stats['shipping_addresses']}")
            click.echo(f"Identical Billing/Shipping: {stats['identical_billing_shipping']}")
            
            # Save detailed results if requested
            if self.output_file:
                results = {
                    'stats': stats,
                    'processed_rows': len(processed_df),
                    'billing_ids': processed_df['billing_address_id'].dropna().tolist(),
                    'shipping_ids': processed_df['shipping_address_id'].dropna().tolist()
                }
                with open(self.output_file, 'w') as f:
                    json.dump(results, f, indent=2)
                click.echo(f"\nDetailed results saved to {self.output_file}")
                
        finally:
            session.close()

class ProcessCustomersCommand(FileInputCommand):
    """Command to process customer records."""
    
    @command_error_handler
    def execute(self) -> None:
        """Execute the command."""
        self.logger.info(f"Processing {self.input_file} for customers...")
        
        # Read the CSV file with Windows encoding
        df = pd.read_csv(
            self.input_file,
            encoding='cp1252',
            dtype=str,  # Read all columns as strings to preserve IDs
            skipinitialspace=True
        )
        total_rows = len(df)
        
        # Debug: Print DataFrame columns and first row
        self.logger.debug(f"DataFrame columns: {df.columns.tolist()}")
        self.logger.debug(f"First row: {df.iloc[0].to_dict()}")
        
        print(f"\nProcessing {total_rows} rows in batches of 100", flush=True)
        
        # Step 1: Process Companies
        print("\n=== Processing Companies ===", flush=True)
        company_processor = CompanyProcessor({'database_url': self.config.database_url})
        df = company_processor.process(df)
        company_stats = company_processor.get_stats()
        click.echo(f"Companies processed: {company_stats['total_processed']}")
        click.echo(f"Successful batches: {company_stats['successful_batches']}")
        click.echo(f"Failed batches: {company_stats['failed_batches']}")
        click.echo(f"Total errors: {company_stats['total_errors']}")
        
        # Step 2: Process Addresses
        print("\n=== Processing Addresses ===", flush=True)
        session = self.get_session()
        try:
            address_processor = AddressProcessor({'database_url': self.config.database_url})
            df = address_processor.process(df)
            address_stats = address_processor.get_stats()
            click.echo(f"Addresses processed: {address_stats['total_processed']}")
            click.echo(f"Successful batches: {address_stats['successful_batches']}")
            click.echo(f"Failed batches: {address_stats['failed_batches']}")
            click.echo(f"Total errors: {address_stats['total_errors']}")
            
            # Step 3: Process Customers
            print("\n=== Processing Customers ===", flush=True)
            customer_processor = CustomerProcessor({'database_url': self.config.database_url})
            processed_df = customer_processor.process(df)
            customer_stats = customer_processor.get_stats()
            
            click.echo("\nCustomer Processing Summary:")
            click.echo(f"Total Rows Processed: {customer_stats['total_processed']}")
            click.echo(f"Successful Batches: {customer_stats['successful_batches']}")
            click.echo(f"Failed Batches: {customer_stats['failed_batches']}")
            click.echo(f"Customers Created: {customer_stats['customers_created']}")
            click.echo(f"Customers Updated: {customer_stats['customers_updated']}")
            click.echo(f"Missing Company Domains: {customer_stats['missing_company_domains']}")
            click.echo(f"Invalid Billing Addresses: {customer_stats['invalid_billing_addresses']}")
            click.echo(f"Invalid Shipping Addresses: {customer_stats['invalid_shipping_addresses']}")
            click.echo(f"Total Errors: {customer_stats['total_errors']}")

            # Step 4: Process Emails
            print("\n=== Processing Emails ===", flush=True)
            email_processor = EmailProcessor({'database_url': self.config.database_url})
            processed_df = email_processor.process(processed_df)
            email_stats = email_processor.get_stats()
            
            click.echo("\nEmail Processing Summary:")
            click.echo(f"Total Emails Processed: {email_stats['emails_processed']}")
            click.echo(f"Valid Emails Stored: {email_stats['emails_stored']}")

            # Step 5: Process Phones
            print("\n=== Processing Phones ===", flush=True)
            phone_processor = PhoneProcessor({'database_url': self.config.database_url})
            processed_df = phone_processor.process(processed_df)
            phone_stats = phone_processor.get_stats()
            
            click.echo("\nPhone Processing Summary:")
            click.echo(f"Total Phone Numbers Processed: {phone_stats['phones_processed']}")
            click.echo(f"Valid Phone Numbers Stored: {phone_stats['phones_stored']}")
            
            # Save detailed results if requested
            if self.output_file:
                results = {
                    'company_stats': company_stats,
                    'address_stats': address_stats,
                    'customer_stats': customer_stats,
                    'email_stats': email_stats,
                    'phone_stats': phone_stats,
                    'processed_rows': len(processed_df),
                    'customer_ids': processed_df['customer_id'].dropna().tolist()
                }
                with open(self.output_file, 'w') as f:
                    json.dump(results, f, indent=2)
                click.echo(f"\nDetailed results saved to {self.output_file}")
                
        finally:
            session.close()

class ProcessEmailsCommand(FileInputCommand):
    """Command to process customer email information."""
    
    @command_error_handler
    def execute(self) -> None:
        """Execute the command."""
        self.logger.info(f"Processing {self.input_file} for customer emails...")
        
        # Read the CSV file with Windows encoding
        df = pd.read_csv(
            self.input_file,
            encoding='cp1252',
            dtype=str,  # Read all columns as strings to preserve IDs
            skipinitialspace=True
        )
        
        session = self.get_session()
        try:
            # Get customer IDs from the processed customers
            customer_processor = CustomerProcessor(session)
            customer_df = customer_processor.process(df)
            
            # Process emails for each customer
            email_processor = EmailProcessor(session)
            total_processed = 0
            total_stored = 0
            
            for _, row in customer_df.iterrows():
                if pd.isna(row['customer_id']):
                    continue
                    
                # Combine all email fields
                email_fields = ['Main Email', 'CC Email', 'Work Email']
                emails = []
                for field in email_fields:
                    if field in row and pd.notna(row[field]):
                        emails.append(str(row[field]))
                email_data = ';'.join(emails)
                processed, stored = email_processor.process_customer_emails(row['customer_id'], email_data)
                total_processed += processed
                total_stored += stored
            
            # Display summary
            click.echo("\nEmail Processing Summary:")
            click.echo(f"Total Emails Processed: {total_processed}")
            click.echo(f"Valid Emails Stored: {total_stored}")
            
            # Save detailed results if requested
            if self.output_file:
                results = {
                    'stats': {
                        'total_processed': total_processed,
                        'total_stored': total_stored
                    }
                }
                with open(self.output_file, 'w') as f:
                    json.dump(results, f, indent=2)
                click.echo(f"\nDetailed results saved to {self.output_file}")
                
        finally:
            session.close()

class ProcessPhonesCommand(FileInputCommand):
    """Command to process customer phone information."""
    
    @command_error_handler
    def execute(self) -> None:
        """Execute the command."""
        self.logger.info(f"Processing {self.input_file} for customer phones...")
        
        # Read the CSV file with Windows encoding
        df = pd.read_csv(
            self.input_file,
            encoding='cp1252',
            dtype=str,  # Read all columns as strings to preserve IDs
            skipinitialspace=True
        )
        
        session = self.get_session()
        try:
            # Get customer IDs from the processed customers
            customer_processor = CustomerProcessor(session)
            customer_df = customer_processor.process(df)
            
            # Process phones for each customer
            phone_processor = PhoneProcessor(session)
            total_processed = 0
            total_stored = 0
            
            for _, row in customer_df.iterrows():
                if pd.isna(row['customer_id']):
                    continue
                    
                # Process each phone field
                phone_fields = [
                    'Main Phone',
                    'Alt. Phone',
                    'Work Phone',
                    'Mobile',
                    'Fax'
                ]
                
                for field_name in phone_fields:
                    if field_name in row and pd.notna(row[field_name]):
                        processed, stored = phone_processor.process_customer_phones(
                            row['customer_id'],
                            str(row[field_name]),
                            field_name
                        )
                        total_processed += processed
                        total_stored += stored
            
            # Display summary
            click.echo("\nPhone Processing Summary:")
            click.echo(f"Total Phone Numbers Processed: {total_processed}")
            click.echo(f"Valid Phone Numbers Stored: {total_stored}")
            
            # Save detailed results if requested
            if self.output_file:
                results = {
                    'stats': {
                        'total_processed': total_processed,
                        'total_stored': total_stored
                    }
                }
                with open(self.output_file, 'w') as f:
                    json.dump(results, f, indent=2)
                click.echo(f"\nDetailed results saved to {self.output_file}")
                
        finally:
            session.close()

class VerifyImportCommand(FileInputCommand):
    """Command to verify data integrity after import process."""
    
    def __init__(self, config: Config, output_file: Optional[Path] = None):
        super().__init__(config, Path("."), output_file)  # Dummy path since we don't use files
    
    @command_error_handler
    def execute(self) -> None:
        """Execute the verification."""
        self.logger.info("Verifying import data integrity...")
        
        session = self.get_session()
        try:
            # Run verification
            verifier = ImportVerifier(session)
            results = verifier.verify_import()
            
            # Display summary
            click.echo("\nVerification Summary:")
            summary = results['summary']
            
            click.echo("\nCustomer Statistics:")
            customers = summary['customers']
            click.echo(f"Total Customers: {customers['total']}")
            click.echo(f"With Company: {customers['with_company']}")
            click.echo(f"With Billing Address: {customers['with_billing_address']}")
            click.echo(f"With Shipping Address: {customers['with_shipping_address']}")
            click.echo(f"With Emails: {customers['with_emails']}")
            click.echo(f"With Phones: {customers['with_phones']}")
            
            click.echo("\nOrphaned Records:")
            orphaned = summary['orphaned']
            click.echo(f"Addresses: {orphaned['addresses']}")
            click.echo(f"Emails: {orphaned['emails']}")
            click.echo(f"Phones: {orphaned['phones']}")
            
            click.echo("\nRelationship Issues:")
            relationships = summary['relationships']
            click.echo(f"Invalid Company References: {relationships['invalid_company_refs']}")
            click.echo(f"Invalid Address References: {relationships['invalid_address_refs']}")
            
            # Display detailed issues if any
            if results['relationship_issues']:
                click.echo("\nRelationship Issues Found:")
                for issue in results['relationship_issues']:
                    click.secho(
                        f"- {issue['type']}: Customer {issue['customer_id']}",
                        fg='yellow'
                    )
            
            if results['orphaned_records']:
                click.echo("\nOrphaned Records Found:")
                for record in results['orphaned_records']:
                    click.secho(
                        f"- {record['type']}: {record['details']}",
                        fg='yellow'
                    )
            
            # Save detailed results if requested
            if self.output_file:
                with open(self.output_file, 'w') as f:
                    json.dump(results, f, indent=2)
                click.echo(f"\nDetailed results saved to {self.output_file}")
            
            if not results['success']:
                click.secho("\nVerification completed with issues!", fg='yellow')
            else:
                click.secho("\nVerification completed successfully!", fg='green')
                
        finally:
            session.close()

__all__ = [
    'ListCompaniesCommand',
    'ExtractDomainsCommand',
    'ProcessAddressesCommand',
    'ProcessCustomersCommand',
    'ProcessEmailsCommand',
    'ProcessPhonesCommand',
    'VerifyImportCommand'
]

import click
import os
import logging
import json
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from importer.importer import CSVImporter
from importer.processors.validator import validate_customer_file
from importer.processors.company import CompanyProcessor
from importer.processors.address import AddressProcessor
from importer.processors.customer import CustomerProcessor
from importer.processors.email import EmailProcessor
from importer.processors.phone import PhoneProcessor
import pandas as pd
from dataclasses import dataclass

@dataclass
class Config:
    database_url: str
    chunk_size: int = 1000
    processor_type: str = 'default'

# Configure logging
def setup_logging(log_dir: Path, level: str) -> None:
    """Setup logging configuration."""
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / 'import.log'
    
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

@click.group()
def cli():
    """CSV Importer CLI tool"""
    pass

@cli.command()
@click.option('--limit', type=int, default=10, help='Number of most recent companies to show')
def list_companies(limit: int):
    """List most recent companies in the database."""
    # Load environment variables
    load_dotenv()
    database_url = os.getenv('DATABASE_URL')
    
    if not database_url:
        click.echo("Error: DATABASE_URL not found in environment variables")
        return
    
    try:
        # Create engine and query companies
        engine = create_engine(database_url)
        with engine.connect() as connection:
            # Get total count
            count_result = connection.execute(text('SELECT COUNT(*) FROM "Company"'))
            total_count = count_result.scalar()
            
            # Get limited companies
            companies_result = connection.execute(text("""
                SELECT domain, name, "createdAt" 
                FROM "Company" 
                ORDER BY "createdAt" DESC
                LIMIT :limit
            """), {"limit": limit})
            companies = companies_result.fetchall()
            
        if not companies:
            click.echo("No companies found in database")
            return
            
        click.echo(f"\nMost recent {limit} of {total_count} companies in database:")
        for company in companies:
            click.echo(f"  - {company.domain} ({company.name})")
        
    except Exception as e:
        click.echo(f"Error querying database: {str(e)}")

@cli.command()
def test_connection():
    """Test database connectivity"""
    # Load environment variables
    load_dotenv()
    database_url = os.getenv('DATABASE_URL')
    
    if not database_url:
        click.echo("Error: DATABASE_URL not found in environment variables")
        return
    
    try:
        # Create engine and test connection
        engine = create_engine(database_url)
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            result.fetchone()
        click.echo("Successfully connected to the database!")
    except Exception as e:
        click.echo(f"Error connecting to database: {str(e)}")

@cli.command()
@click.argument('directory', type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path))
@click.option('--log-level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR'], case_sensitive=False), default='INFO')
def import_csv(directory: Path, log_level: str):
    """Import CSV files from the specified directory."""
    # Load environment variables
    load_dotenv()
    database_url = os.getenv('DATABASE_URL')
    
    if not database_url:
        click.echo("Error: DATABASE_URL not found in environment variables")
        return
        
    # Setup logging
    setup_logging(directory / "logs", log_level)
    logger = logging.getLogger(__name__)
    
    try:
        config = Config(database_url=database_url)
        importer = CSVImporter(config)
        importer.process_directory(directory)
        click.echo("Import completed successfully!")
    except Exception as e:
        logger.error(f"Import failed: {e}")
        click.echo(f"Import failed. Check logs for details: {e}")
        raise click.Abort()

@cli.command()
@click.argument('file', type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path))
@click.option('--output', type=click.Path(file_okay=True, dir_okay=False, path_type=Path), help='Save validation results to file')
def validate(file: Path, output: Path | None):
    """Validate a customer CSV file before importing."""
    try:
        click.echo(f"Validating {file}...")
        results = validate_customer_file(file)
        
        # Display summary
        summary = results['summary']
        stats = summary['stats']
        click.echo("\nValidation Summary:")
        click.echo(f"Total Rows: {stats['total_rows']}")
        click.echo(f"Valid Rows: {stats['valid_rows']}")
        click.echo(f"Rows with Warnings: {stats['rows_with_warnings']}")
        click.echo(f"Rows with Errors: {stats['rows_with_errors']}")
        
        # Display errors and warnings
        if summary['errors']:
            click.echo("\nIssues Found:")
            for error in summary['errors']:
                color = 'red' if error['severity'] == 'CRITICAL' else 'yellow' if error['severity'] == 'WARNING' else 'blue'
                click.secho(
                    f"[{error['severity']}] Row {error['row']}, Field: {error['field']} - {error['message']}",
                    fg=color
                )
        
        # Save to file if requested
        if output:
            with open(output, 'w') as f:
                json.dump(results, f, indent=2)
            click.echo(f"\nDetailed results saved to {output}")
        
        if not results['is_valid']:
            raise click.Abort()
            
    except Exception as e:
        click.secho(f"Validation failed: {str(e)}", fg='red')
        raise click.Abort()

@cli.command()
@click.argument('file', type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path))
@click.option('--output', type=click.Path(file_okay=True, dir_okay=False, path_type=Path), help='Save domain extraction results to file')
def extract_domains(file: Path, output: Path | None):
    """Extract and analyze email domains from a customer CSV file."""
    try:
        # First validate the file
        validation_results = validate_customer_file(file)
        if not validation_results['is_valid']:
            click.secho("File validation failed. Please fix validation errors first.", fg='red')
            raise click.Abort()
            
        # Load environment variables
        load_dotenv()
        database_url = os.getenv('DATABASE_URL')
        
        if not database_url:
            click.secho("Error: DATABASE_URL not found in environment variables", fg='red')
            raise click.Abort()
            
        click.echo(f"Processing {file} for email domains...")
        
        # Read the CSV file
        df = pd.read_csv(file)
        
        # Initialize and run company processor
        processor = CompanyProcessor({'database_url': database_url})
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
        if output:
            results = {
                'stats': stats,
                'domains': list(domains),
                'rows_with_domain': len(processed_df[processed_df['company_domain'].str.len() > 0])
            }
            with open(output, 'w') as f:
                json.dump(results, f, indent=2)
            click.echo(f"\nDetailed results saved to {output}")
            
    except Exception as e:
        click.secho(f"Domain extraction failed: {str(e)}", fg='red')
        raise click.Abort()

@cli.command()
@click.argument('file', type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path))
@click.option('--output', type=click.Path(file_okay=True, dir_okay=False, path_type=Path), help='Save address processing results to file')
def process_addresses(file: Path, output: Path | None):
    """Process and deduplicate addresses from a customer CSV file."""
    try:
        # First validate the file
        validation_results = validate_customer_file(file)
        if not validation_results['is_valid']:
            click.secho("File validation failed. Please fix validation errors first.", fg='red')
            raise click.Abort()
            
        # Load environment variables
        load_dotenv()
        database_url = os.getenv('DATABASE_URL')
        
        if not database_url:
            click.secho("Error: DATABASE_URL not found in environment variables", fg='red')
            raise click.Abort()
            
        click.echo(f"Processing {file} for addresses...")
        
        # Create database engine and session
        engine = create_engine(database_url)
        from sqlalchemy.orm import sessionmaker
        Session = sessionmaker(bind=engine)
        session = Session()
        
        try:
            # Read the CSV file
            df = pd.read_csv(file)
            
            # Initialize and run address processor
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
            if output:
                results = {
                    'stats': stats,
                    'processed_rows': len(processed_df),
                    'billing_ids': processed_df['billing_address_id'].dropna().tolist(),
                    'shipping_ids': processed_df['shipping_address_id'].dropna().tolist()
                }
                with open(output, 'w') as f:
                    json.dump(results, f, indent=2)
                click.echo(f"\nDetailed results saved to {output}")
                
        finally:
            session.close()
            
    except Exception as e:
        click.secho(f"Address processing failed: {str(e)}", fg='red')
        raise click.Abort()

@cli.command()
@click.argument('file', type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path))
@click.option('--output', type=click.Path(file_okay=True, dir_okay=False, path_type=Path), help='Save customer processing results to file')
def process_customers(file: Path, output: Path | None):
    """Process customer records from a CSV file."""
    try:
        # First validate the file
        validation_results = validate_customer_file(file)
        if not validation_results['is_valid']:
            click.secho("File validation failed. Please fix validation errors first.", fg='red')
            raise click.Abort()
            
        # Load environment variables
        load_dotenv()
        database_url = os.getenv('DATABASE_URL')
        
        if not database_url:
            click.secho("Error: DATABASE_URL not found in environment variables", fg='red')
            raise click.Abort()
            
        click.echo(f"Processing {file} for customers...")
        
        # Create database engine and session
        engine = create_engine(database_url)
        from sqlalchemy.orm import sessionmaker
        Session = sessionmaker(bind=engine)
        session = Session()
        
        try:
            # Read the CSV file
            df = pd.read_csv(file)
            
            # First run company processor to extract domains
            company_processor = CompanyProcessor({'database_url': database_url})
            df = company_processor.process(df)
            
            # Then run customer processor
            processor = CustomerProcessor(session)
            processed_df = processor.process(df)
            
            # Get statistics
            stats = processor.get_stats()
            
            # Display summary
            click.echo("\nCustomer Processing Summary:")
            click.echo(f"Total Rows Processed: {stats['customers_processed']}")
            click.echo(f"Customers Created: {stats['customers_created']}")
            click.echo(f"Missing Company Domains: {stats['missing_company_domains']}")
            click.echo(f"Invalid Billing Addresses: {stats['invalid_billing_addresses']}")
            click.echo(f"Invalid Shipping Addresses: {stats['invalid_shipping_addresses']}")
            click.echo(f"Errors: {stats['errors']}")
            
            # Save detailed results if requested
            if output:
                results = {
                    'stats': stats,
                    'processed_rows': len(processed_df),
                    'customer_ids': processed_df['customer_id'].dropna().tolist()
                }
                with open(output, 'w') as f:
                    json.dump(results, f, indent=2)
                click.echo(f"\nDetailed results saved to {output}")
                
        finally:
            session.close()
            
    except Exception as e:
        click.secho(f"Customer processing failed: {str(e)}", fg='red')
        raise click.Abort()

@cli.command()
@click.argument('file', type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path))
@click.option('--output', type=click.Path(file_okay=True, dir_okay=False, path_type=Path), help='Save email processing results to file')
def process_emails(file: Path, output: Path | None):
    """Process and store customer email information from a CSV file."""
    try:
        # First validate the file
        validation_results = validate_customer_file(file)
        if not validation_results['is_valid']:
            click.secho("File validation failed. Please fix validation errors first.", fg='red')
            raise click.Abort()
            
        # Load environment variables
        load_dotenv()
        database_url = os.getenv('DATABASE_URL')
        
        if not database_url:
            click.secho("Error: DATABASE_URL not found in environment variables", fg='red')
            raise click.Abort()
            
        click.echo(f"Processing {file} for customer emails...")
        
        # Create database engine and session
        engine = create_engine(database_url)
        from sqlalchemy.orm import sessionmaker
        Session = sessionmaker(bind=engine)
        session = Session()
        
        try:
            # Read the CSV file
            df = pd.read_csv(file)
            
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
            if output:
                results = {
                    'stats': {
                        'total_processed': total_processed,
                        'total_stored': total_stored
                    }
                }
                with open(output, 'w') as f:
                    json.dump(results, f, indent=2)
                click.echo(f"\nDetailed results saved to {output}")
                
        finally:
            session.close()
            
    except Exception as e:
        click.secho(f"Email processing failed: {str(e)}", fg='red')
        raise click.Abort()

@cli.command()
@click.argument('file', type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path))
@click.option('--output', type=click.Path(file_okay=True, dir_okay=False, path_type=Path), help='Save phone processing results to file')
def process_phones(file: Path, output: Path | None):
    """Process and store customer phone information from a CSV file."""
    try:
        # First validate the file
        validation_results = validate_customer_file(file)
        if not validation_results['is_valid']:
            click.secho("File validation failed. Please fix validation errors first.", fg='red')
            raise click.Abort()
            
        # Load environment variables
        load_dotenv()
        database_url = os.getenv('DATABASE_URL')
        
        if not database_url:
            click.secho("Error: DATABASE_URL not found in environment variables", fg='red')
            raise click.Abort()
            
        click.echo(f"Processing {file} for customer phones...")
        
        # Create database engine and session
        engine = create_engine(database_url)
        from sqlalchemy.orm import sessionmaker
        Session = sessionmaker(bind=engine)
        session = Session()
        
        try:
            # Read the CSV file
            df = pd.read_csv(file)
            
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
            if output:
                results = {
                    'stats': {
                        'total_processed': total_processed,
                        'total_stored': total_stored
                    }
                }
                with open(output, 'w') as f:
                    json.dump(results, f, indent=2)
                click.echo(f"\nDetailed results saved to {output}")
                
        finally:
            session.close()
            
    except Exception as e:
        click.secho(f"Phone processing failed: {str(e)}", fg='red')
        raise click.Abort()

if __name__ == '__main__':
    cli()

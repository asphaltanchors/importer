"""
Main entry point for running the CLI as a module.
"""

import click
from pathlib import Path

from .config import Config
from .logging import setup_logging
from ..commands.validate import ValidateCustomersCommand, ValidateSalesCommand
from ..commands.utils import TestConnectionCommand
from ..commands.products import ProcessProductsCommand
from ..commands.invoices import ValidateInvoiceCommand
from ..commands.customers import (
    ListCompaniesCommand,
    ExtractDomainsCommand,
    ProcessAddressesCommand,
    ProcessCustomersCommand,
    ProcessEmailsCommand,
    ProcessPhonesCommand,
    VerifyImportCommand
)

@click.group()
def cli():
    """CSV Importer CLI tool"""
    pass

@cli.command()
def test_connection():
    """Test database connectivity"""
    try:
        config = Config.from_env()
        command = TestConnectionCommand(config)
        command.execute()
    except Exception as e:
        click.secho(f"Error: {str(e)}", fg='red')
        raise click.Abort()

@cli.command()
@click.argument('file', type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path))
@click.option('--output', type=click.Path(file_okay=True, dir_okay=False, path_type=Path), help='Save validation results to file')
@click.option('--log-level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR'], case_sensitive=False), default='INFO')
def validate(file: Path, output: Path | None, log_level: str):
    """Validate a customer CSV file before importing."""
    try:
        # Setup logging
        setup_logging(level=log_level)
        
        # Initialize and run command
        config = Config.from_env()
        command = ValidateCustomersCommand(config, file, output)
        command.execute()
        
    except Exception as e:
        click.secho(f"Error: {str(e)}", fg='red')
        raise click.Abort()

@cli.command()
@click.argument('file', type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path))
@click.option('--output', type=click.Path(file_okay=True, dir_okay=False, path_type=Path), help='Save validation results to file')
@click.option('--log-level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR'], case_sensitive=False), default='INFO')
def validate_sales(file: Path, output: Path | None, log_level: str):
    """Validate a sales CSV file before importing."""
    try:
        # Setup logging
        setup_logging(level=log_level)
        
        # Initialize and run command
        config = Config.from_env()
        command = ValidateSalesCommand(config, file, output)
        command.execute()
        
    except Exception as e:
        click.secho(f"Error: {str(e)}", fg='red')
        raise click.Abort()

# Customer Commands Group
@cli.group()
def customers():
    """Customer data management commands"""
    pass

@customers.command('list-companies')
@click.option('--limit', type=int, default=10, help='Number of most recent companies to show')
def list_companies(limit: int):
    """List most recent companies in the database."""
    try:
        config = Config.from_env()
        command = ListCompaniesCommand(config, limit)
        command.execute()
    except Exception as e:
        click.secho(f"Error: {str(e)}", fg='red')
        raise click.Abort()

@customers.command('extract-domains')
@click.argument('file', type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path))
@click.option('--output', type=click.Path(file_okay=True, dir_okay=False, path_type=Path), help='Save domain extraction results to file')
def extract_domains(file: Path, output: Path | None):
    """Extract and analyze email domains from a customer CSV file."""
    try:
        config = Config.from_env()
        command = ExtractDomainsCommand(config, file, output)
        command.execute()
    except Exception as e:
        click.secho(f"Error: {str(e)}", fg='red')
        raise click.Abort()

@customers.command('process-addresses')
@click.argument('file', type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path))
@click.option('--output', type=click.Path(file_okay=True, dir_okay=False, path_type=Path), help='Save address processing results to file')
def process_addresses(file: Path, output: Path | None):
    """Process and deduplicate addresses from a customer CSV file."""
    try:
        config = Config.from_env()
        command = ProcessAddressesCommand(config, file, output)
        command.execute()
    except Exception as e:
        click.secho(f"Error: {str(e)}", fg='red')
        raise click.Abort()

@customers.command('process')
@click.argument('file', type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path))
@click.option('--output', type=click.Path(file_okay=True, dir_okay=False, path_type=Path), help='Save customer processing results to file')
def process_customers(file: Path, output: Path | None):
    """Process customer records from a CSV file."""
    try:
        config = Config.from_env()
        command = ProcessCustomersCommand(config, file, output)
        command.execute()
    except Exception as e:
        click.secho(f"Error: {str(e)}", fg='red')
        raise click.Abort()

@customers.command('process-emails')
@click.argument('file', type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path))
@click.option('--output', type=click.Path(file_okay=True, dir_okay=False, path_type=Path), help='Save email processing results to file')
def process_emails(file: Path, output: Path | None):
    """Process and store customer email information from a CSV file."""
    try:
        config = Config.from_env()
        command = ProcessEmailsCommand(config, file, output)
        command.execute()
    except Exception as e:
        click.secho(f"Error: {str(e)}", fg='red')
        raise click.Abort()

@customers.command('process-phones')
@click.argument('file', type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path))
@click.option('--output', type=click.Path(file_okay=True, dir_okay=False, path_type=Path), help='Save phone processing results to file')
def process_phones(file: Path, output: Path | None):
    """Process and store customer phone information from a CSV file."""
    try:
        config = Config.from_env()
        command = ProcessPhonesCommand(config, file, output)
        command.execute()
    except Exception as e:
        click.secho(f"Error: {str(e)}", fg='red')
        raise click.Abort()

@customers.command('verify')
@click.option('--output', type=click.Path(file_okay=True, dir_okay=False, path_type=Path), help='Save verification results to file')
def verify_import(output: Path | None):
    """Verify data integrity after import process."""
    try:
        config = Config.from_env()
        command = VerifyImportCommand(config, output)
        command.execute()
    except Exception as e:
        click.secho(f"Error: {str(e)}", fg='red')
        raise click.Abort()

# Products Commands Group
@cli.group()
def products():
    """Product data management commands"""
    pass

@products.command('process')
@click.argument('file', type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path))
@click.option('--output', type=click.Path(file_okay=True, dir_okay=False, path_type=Path), help='Save product processing results to file')
@click.option('--log-level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR'], case_sensitive=False), default='INFO')
def process_products(file: Path, output: Path | None, log_level: str):
    """Process products from a sales data file."""
    try:
        # Setup logging
        setup_logging(level=log_level)
        
        # Initialize and run command
        config = Config.from_env()
        command = ProcessProductsCommand(config, file, output)
        command.execute()
    except Exception as e:
        click.secho(f"Error: {str(e)}", fg='red')
        raise click.Abort()

# Invoices Commands Group
@cli.group()
def invoices():
    """Invoice data management commands"""
    pass

@invoices.command('validate')
@click.argument('file', type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path))
@click.option('--output', type=click.Path(file_okay=True, dir_okay=False, path_type=Path), help='Save validation results to file')
@click.option('--log-level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR'], case_sensitive=False), default='INFO')
def validate_invoice(file: Path, output: Path | None, log_level: str):
    """Validate an invoice CSV file before importing."""
    try:
        # Setup logging
        setup_logging(level=log_level)
        
        # Initialize and run command
        config = Config.from_env()
        command = ValidateInvoiceCommand(config, file, output)
        command.execute()
    except Exception as e:
        click.secho(f"Error: {str(e)}", fg='red')
        raise click.Abort()

if __name__ == '__main__':
    cli()

"""
Core CLI implementation for the importer package.
"""

import click
from pathlib import Path

from .config import Config
from .logging import setup_logging, get_logger
from ..commands.validate import ValidateCustomersCommand, ValidateSalesCommand
from ..commands.utils import TestConnectionCommand
from ..commands.sales import sales, process_invoices, process_receipts, import_products
from ..commands.verify import VerifyCommand
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
@click.option('--debug', is_flag=True, help='Enable detailed debug output')
@click.pass_context
def cli(ctx, debug: bool):
    """CSV Importer CLI tool"""
    # Store debug flag in context for subcommands
    ctx.ensure_object(dict)
    ctx.obj['debug'] = debug
    
    # Setup logging with debug flag
    setup_logging(debug=debug)
    
    # Get logger for CLI
    logger = get_logger('cli')
    if debug:
        logger.debug("Debug mode enabled")
        
    # Initialize config and store in context
    try:
        config = Config.from_env()
        ctx.obj['config'] = config
        if debug:
            logger.debug(f"Using database: {config.database_url}")
    except Exception as e:
        click.echo(f"Error initializing configuration: {str(e)}", err=True)
        ctx.exit(1)

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
def validate(file: Path, output: Path | None):
    """Validate a customer CSV file before importing."""
    try:
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
def validate_sales(file: Path, output: Path | None):
    """Validate a sales CSV file before importing."""
    try:
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
@click.pass_context
def process_customers(ctx, file: Path, output: Path | None):
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

# Register top-level process commands
cli.add_command(process_invoices)
cli.add_command(process_receipts)
cli.add_command(import_products)

# Register sales commands (specialized operations)
cli.add_command(sales)

# Register verify commands
verify_command = VerifyCommand()
for command in verify_command.commands:
    cli.add_command(command)

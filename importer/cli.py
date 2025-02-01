import click
import os
import logging
import json
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from .importer import CSVImporter
from .processors.validator import validate_customer_file
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

if __name__ == '__main__':
    cli()

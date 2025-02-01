import click
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from .importer import CSVImporter
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

if __name__ == '__main__':
    cli()

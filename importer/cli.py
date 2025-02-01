import click
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

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

if __name__ == '__main__':
    cli()

"""
Utility commands for the importer CLI.
Provides helper commands for system operations and diagnostics.
"""

import click
from sqlalchemy import text

from ...cli.base import BaseCommand, command_error_handler
from ...cli.config import Config

class TestConnectionCommand(BaseCommand):
    """Command to test database connectivity."""
    
    def __init__(self, config: Config):
        super().__init__(config)
    
    @command_error_handler
    def execute(self) -> None:
        """Execute the connection test."""
        self.logger.info("Testing database connection...")
        
        try:
            # Get a session and test connection
            session = self.get_session()
            with session:
                result = session.execute(text("SELECT 1"))
                result.scalar()
                
            click.secho(
                "Successfully connected to the database!",
                fg='green'
            )
            
        except Exception as e:
            self.logger.error(f"Connection failed: {str(e)}")
            raise click.Abort()

__all__ = ['TestConnectionCommand']

"""
Command module for verification operations.
"""

from typing import Optional
from pathlib import Path

import click

from ...processors.verifier import SalesVerifier
from ...cli.base import BaseCommand


class VerifyCommand(BaseCommand):
    """Command for verifying imported sales data."""

    def __init__(self, config=None):
        from ...cli.config import Config
        super().__init__(config or Config.from_env())

    def execute(self) -> None:
        """Execute the command."""
        pass  # Base command execution, not used directly

    @click.group(name="verify")
    def verify():
        """Verify imported data integrity."""
        pass

    @verify.command()
    @click.argument("file", type=click.Path(exists=True, path_type=Path))
    @click.option('--log-level', type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR'], case_sensitive=False), default='INFO')
    @staticmethod
    def sales(file: Path, log_level: str):
        """Verify sales data integrity for the given file."""
        from ...cli.config import Config
        from ...cli.logging import setup_logging

        try:
            # Setup logging
            setup_logging(level=log_level)
            
            # Initialize and run verifier with config
            config = Config.from_env()
            verifier = SalesVerifier({'database_url': config.database_url})
            verifier.verify(file)
            
        except Exception as e:
            click.secho(f"Error: {str(e)}", fg='red')
            raise click.Abort()

    @property
    def commands(self):
        return [self.verify]

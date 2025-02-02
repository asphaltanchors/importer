"""
Command module for verification operations.
"""

from typing import Optional
from pathlib import Path

import click

from ...processors.sales_verifier import SalesVerifier
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
    @click.option('--output', type=click.Path(file_okay=True, dir_okay=False, path_type=Path), help='Save verification results to file')
    @staticmethod
    def sales(file: Path, log_level: str, output: Path | None):
        """Verify sales data integrity for the given file.
        
        This command performs comprehensive verification of sales data:
        - Validates customer and product references
        - Verifies line item integrity and calculations
        - Checks order totals and payment status
        - Detects orphaned records
        - Ensures data consistency across all related records
        """
        from ...cli.config import Config
        from ...cli.logging import setup_logging
        import json

        try:
            # Setup logging
            setup_logging(level=log_level)
            
            # Initialize and run verifier with config
            config = Config.from_env()
            verifier = SalesVerifier({'database_url': config.database_url})
            verifier.verify(file)
            
            # Save results if output file specified
            if output and verifier.issues:
                results = {
                    'success': len(verifier.issues) == 0,
                    'stats': verifier.stats,
                    'issues': [
                        {
                            'type': issue['type'],
                            'message': issue['message']
                        }
                        for issue in verifier.issues
                    ]
                }
                with open(output, 'w') as f:
                    json.dump(results, f, indent=2)
            
        except Exception as e:
            click.secho(f"Error: {str(e)}", fg='red')
            raise click.Abort()

    @property
    def commands(self):
        return [self.verify]

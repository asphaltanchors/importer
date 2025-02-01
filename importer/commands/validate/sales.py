"""Sales validation command."""

import click
import json
from pathlib import Path
from typing import Optional, Dict, Any

from ...cli.base import FileInputCommand, command_error_handler
from ...cli.config import Config
from ...processors.sales_validator import validate_sales_file

class ValidateSalesCommand(FileInputCommand):
    """Command to validate sales CSV files."""
    
    def __init__(self, config: Config, input_file: Path, output_file: Optional[Path] = None):
        super().__init__(config, input_file, output_file)
    
    @command_error_handler
    def execute(self) -> None:
        """Execute the validation command."""
        self.logger.info(f"Validating sales file: {self.input_file}...")
        
        # Run validation
        results = validate_sales_file(self.input_file)
        
        # Display summary
        self._display_summary(results)
        
        # Save detailed results if requested
        if self.output_file:
            self._save_results(results)
            
        if not results['is_valid']:
            raise click.Abort()
    
    def _display_summary(self, results: Dict[str, Any]) -> None:
        """Display validation summary to console."""
        summary = results['summary']
        stats = summary['stats']
        
        click.echo("\nSales Validation Summary:")
        click.echo(f"Total Rows: {stats['total_rows']}")
        click.echo(f"Valid Rows: {stats['valid_rows']}")
        click.echo(f"Rows with Warnings: {stats['rows_with_warnings']}")
        click.echo(f"Rows with Errors: {stats['rows_with_errors']}")
        
        if summary['errors']:
            click.echo("\nIssues Found:")
            for error in summary['errors']:
                color = 'red' if error['severity'] == 'CRITICAL' else 'yellow'
                click.secho(
                    f"[{error['severity']}] Row {error['row']}, "
                    f"Field: {error['field']} - {error['message']}",
                    fg=color
                )
    
    def _save_results(self, results: Dict[str, Any]) -> None:
        """Save validation results to file."""
        with open(self.output_file, 'w') as f:
            json.dump(results, f, indent=2)
        click.echo(f"\nDetailed results saved to {self.output_file}")

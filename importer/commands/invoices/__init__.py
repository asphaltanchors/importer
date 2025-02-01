"""Invoice processing commands."""

from pathlib import Path
from typing import Optional

from ...cli.base import FileInputCommand
from ...cli.config import Config
from ...processors.invoice_validator import validate_invoice_file

class ValidateInvoiceCommand(FileInputCommand):
    """Command to validate invoice CSV files."""
    
    name = 'validate-invoice'
    help = 'Validate an invoice CSV file'

    def __init__(self, config: Config, input_file: Path, output_file: Optional[Path] = None):
        """Initialize command.
        
        Args:
            config: Application configuration
            input_file: Path to input CSV file
            output_file: Optional path to save validation results
        """
        super().__init__(config, input_file, output_file)

    def execute(self) -> Optional[int]:
        """Execute the validation command.
        
        Returns:
            Optional exit code
        """
        self.logger.info(f"Validating invoice file: {self.input_file}")
        
        # Run invoice-specific validation
        results = validate_invoice_file(self.input_file, self.config.database_url)
        
        # Print summary
        stats = results['summary']['stats']
        self.logger.info(f"\nValidation Summary:")
        self.logger.info(f"Total Rows: {stats['total_rows']}")
        self.logger.info(f"Valid Rows: {stats['valid_rows']}")
        self.logger.info(f"Rows with Warnings: {stats['rows_with_warnings']}")
        self.logger.info(f"Rows with Errors: {stats['rows_with_errors']}")
        
        if results['summary']['errors']:
            self.logger.warning("\nValidation Issues:")
            for error in results['summary']['errors']:
                severity = error['severity']
                message = (f"[{severity}] Row {error['row']}, "
                         f"Field: {error['field']} - {error['message']}")
                if severity == 'CRITICAL':
                    self.logger.error(message)
                else:
                    self.logger.warning(message)
            
            if not results['is_valid']:
                return 1
        
        return 0

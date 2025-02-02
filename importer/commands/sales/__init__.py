"""Invoice processing commands."""

from pathlib import Path
from typing import Optional, List

from .products import ProcessProductsCommand
from .line_items import ProcessLineItemsCommand
from .orders import ProcessOrdersCommand
from .payments import ProcessPaymentsCommand

from ...cli.base import FileInputCommand
from ...cli.config import Config
from ...processors.invoice import InvoiceProcessor
from ...processors.invoice_validator import validate_invoice_file

class ValidateInvoiceCommand(FileInputCommand):
    """Validate an invoice file before processing."""
    
    name = 'validate-invoice'
    help = 'Validate an invoice file before processing'

    def __init__(self, config: Config, input_file: Path, output_file: Optional[Path] = None):
        """Initialize command.
        
        Args:
            config: Application configuration
            input_file: Path to input CSV file
            output_file: Optional path to save results
        """
        super().__init__(config, input_file, output_file)

    def execute(self) -> Optional[int]:
        """Execute the command.
        
        Returns:
            Optional exit code
        """
        results = validate_invoice_file(self.input_file, self.config.database_url)
        
        # Print summary
        stats = results['summary']['stats']
        self.logger.info(f"Validated {stats['total_rows']} rows:")
        self.logger.info(f"  Valid rows: {stats['valid_rows']}")
        if stats['rows_with_warnings'] > 0:
            self.logger.info(f"  Rows with warnings: {stats['rows_with_warnings']}")
        if stats['rows_with_errors'] > 0:
            self.logger.info(f"  Rows with errors: {stats['rows_with_errors']}")
        
        if results['summary']['errors']:
            self.logger.warning("Issues found:")
            for error in results['summary']['errors']:
                if error['severity'] == 'WARNING':
                    self.logger.warning(f"  {error['message']}")
                else:
                    self.logger.error(f"  {error['message']}")
            
            # Only return error code for actual errors, not warnings
            if not results['is_valid']:
                return 1
            
        return 0

class SalesProcessCommand(FileInputCommand):
    """Process a sales data file in the correct sequence."""
    
    name = 'process'
    help = 'Process a sales data file (products, line items, orders, payments)'

    def __init__(self, config: Config, input_file: Path, output_file: Optional[Path] = None):
        """Initialize command.
        
        Args:
            config: Application configuration
            input_file: Path to input CSV file
            output_file: Optional path to save results
        """
        super().__init__(config, input_file, output_file)

    def execute(self) -> Optional[int]:
        """Execute the command sequence.
        
        Returns:
            Optional exit code
        """
        # Process products first
        self.logger.info("Running process-products...")
        products_command = ProcessProductsCommand(self.config, self.input_file, self.output_file)
        result = products_command.execute()
        if result is not None and result != 0:
            return result

        # Process orders
        self.logger.info("Running process-orders...")
        orders_command = ProcessOrdersCommand(self.config, self.input_file, self.output_file)
        result = orders_command.execute()
        if result is not None and result != 0:
            return result

        # Process line items
        self.logger.info("Running process-line-items...")
        line_items_command = ProcessLineItemsCommand(self.config, self.input_file, self.output_file)
        result = line_items_command.execute()
        if result is not None and result != 0:
            return result

        # Process payments
        self.logger.info("Running process-payments...")
        payments_command = ProcessPaymentsCommand(self.config, self.input_file, self.output_file)
        result = payments_command.execute()
        if result is not None and result != 0:
            return result

        return 0

class ProcessInvoicesCommand(FileInputCommand):
    """Process invoices from a sales data file."""
    
    name = 'process-invoices'
    help = 'Process invoices from a sales data file'

    def __init__(self, config: Config, input_file: Path, output_file: Optional[Path] = None):
        """Initialize command.
        
        Args:
            config: Application configuration
            input_file: Path to input CSV file
            output_file: Optional path to save results
        """
        super().__init__(config, input_file, output_file)

    def execute(self) -> Optional[int]:
        """Execute the command.
        
        Returns:
            Optional exit code
        """
        processor = InvoiceProcessor(self.config.database_url)
        results = processor.process(self.input_file)
        
        # Print summary
        stats = results['summary']['stats']
        self.logger.info(f"Processed {stats['total_invoices']} invoices:")
        self.logger.info(f"  Created: {stats['created']}")
        self.logger.info(f"  Updated: {stats.get('updated', 0)}")
        self.logger.info(f"  Line items: {stats.get('line_items', 0)}")
        self.logger.info(f"  Errors: {stats['errors']}")
        
        if results['summary']['errors']:
            self.logger.warning("Errors encountered:")
            for error in results['summary']['errors']:
                severity = error.get('severity', 'ERROR')
                if severity == 'WARNING':
                    self.logger.warning(f"  {error['message']}")
                else:
                    self.logger.error(f"  {error['message']}")
            
            # Only return error code for actual errors, not warnings
            if stats['errors'] > 0:
                return 1
            
        return 0

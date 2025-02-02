"""Process invoices command."""

from pathlib import Path
import click
import json
from typing import Optional
import pandas as pd

from ...cli.base import FileInputCommand, command_error_handler
from ...cli.logging import get_logger
from ...utils.csv_normalization import normalize_dataframe_columns
from ...processors.invoice import InvoiceProcessor
from ...processors.line_item import LineItemProcessor
from ...db.session import SessionManager

class ProcessInvoicesCommand(FileInputCommand):
    """Command to process invoices from a sales data file."""
    
    def __init__(self, config, input_file: Path, output_file: Optional[Path] = None, batch_size: int = 100):
        """Initialize the command.
        
        Args:
            config: Application configuration
            input_file: Path to input CSV file
            output_file: Optional path to save results
            batch_size: Number of records to process per batch
        """
        super().__init__(config, input_file, output_file)
        self.batch_size = batch_size
        self.logger = get_logger(__name__)
    
    @command_error_handler
    def execute(self) -> None:
        """Execute the command."""
        session_manager = SessionManager(self.config.database_url)
        
        try:
            self.logger.info(f"Processing invoices from {self.input_file}")
            self.logger.info(f"Batch size: {self.batch_size}")
            
            # Process invoices first
            with session_manager.get_session() as session:
                # Read CSV file
                self.logger.info("Reading CSV file...")
                if self.debug:
                    self.logger.debug(f"Reading CSV from {self.input_file}")
                df = pd.read_csv(self.input_file)
                if self.debug:
                    self.logger.debug("Normalizing dataframe columns")
                df = normalize_dataframe_columns(df)
                self.logger.info(f"Found {len(df)} rows")
                
                if self.debug:
                    self.logger.debug("Initializing InvoiceProcessor")
                invoice_processor = InvoiceProcessor(session, self.batch_size)
                invoice_processor.debug = self.debug  # Pass debug flag from command
                processed_df = invoice_processor.process(df)
                
                # Convert processor results to our standard format
                invoice_result = {
                    'success': invoice_processor.stats['failed_batches'] == 0,
                    'summary': {
                        'stats': invoice_processor.stats
                    }
                }
                
                if not invoice_result['success']:
                    self.logger.error("Failed to process invoices")
                    if self.debug:
                        self.logger.debug(f"Failed batches: {invoice_processor.stats['failed_batches']}")
                    return
                
                self.logger.info("Invoice processing complete")
                if self.debug:
                    self.logger.debug(f"Created: {invoice_result['summary']['stats']['created']}")
                    self.logger.debug(f"Updated: {invoice_result['summary']['stats']['updated']}")
                    self.logger.debug(f"Errors: {invoice_result['summary']['stats']['errors']}")
            
            # Then process line items
            self.logger.info("Processing line items")
            if self.debug:
                self.logger.debug("Initializing LineItemProcessor")
            line_item_processor = LineItemProcessor(session_manager, self.batch_size)
            line_item_processor.debug = self.debug  # Pass debug flag from command
            line_item_result = line_item_processor.process_file(str(self.input_file))
            
            if not line_item_result['success']:
                self.logger.error("Failed to process line items")
                return
                
            self.logger.info("Line item processing complete")
            if self.debug:
                self.logger.debug(f"Total line items: {line_item_result['summary']['stats']['total_line_items']}")
                self.logger.debug(f"Orders processed: {line_item_result['summary']['stats']['orders_processed']}")
                self.logger.debug(f"Products not found: {line_item_result['summary']['stats']['products_not_found']}")
                self.logger.debug(f"Orders not found: {line_item_result['summary']['stats']['orders_not_found']}")
            
            # Save results if output file specified
            if self.output_file:
                if self.debug:
                    self.logger.debug(f"Saving results to {self.output_file}")
                results = {
                    'invoice_processing': invoice_result,
                    'line_item_processing': line_item_result
                }
                with open(self.output_file, 'w') as f:
                    json.dump(results, f, indent=2)
                self.logger.info(f"Detailed results saved to {self.output_file}")
                
        except Exception as e:
            self.logger.error(f"Failed to process file: {str(e)}", exc_info=self.debug)
            return

# Click command wrapper
@click.command()
@click.argument('file_path', type=click.Path(exists=True, path_type=Path))
@click.option('--output', type=click.Path(file_okay=True, dir_okay=False, path_type=Path), help='Save processing results to file')
@click.option('--batch-size', default=100, help='Number of records to process per batch')
@click.pass_context
def process_invoices(ctx, file_path: Path, output: Optional[Path], batch_size: int):
    """Process invoices from a CSV file."""
    config = ctx.obj.get('config')
    if not config:
        logger = get_logger(__name__)
        logger.error("No configuration found in context")
        return
        
    command = ProcessInvoicesCommand(config, file_path, output, batch_size)
    command.execute()

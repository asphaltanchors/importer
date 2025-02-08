"""Process sales receipts from a CSV file.

A top-level command for importing sales receipt data into the database. This command:
1. Processes companies from receipt data
2. Creates/updates customer records
3. Creates/updates receipt records
4. Creates/updates product records
5. Processes line items with product mapping

The processing sequence ensures proper relationship handling:
- Companies must exist before customer records
- Customers must exist before receipt records
- Products must exist before line items
- Line items link orders with products
"""

from pathlib import Path
import click
import json
from typing import Optional
import pandas as pd

from ...cli.base import FileInputCommand, command_error_handler
from ...cli.logging import get_logger
from ...utils.csv_normalization import normalize_dataframe_columns
from ...processors.sales_receipt import SalesReceiptProcessor
from ...processors.sales_receipt_line_item import SalesReceiptLineItemProcessor
from ...processors.company import CompanyProcessor
from ...processors.product import ProductProcessor
from ...db.session import SessionManager

class ProcessReceiptsCommand(FileInputCommand):
    """Command to process sales receipts from a sales data file."""
    
    def __init__(self, config, input_file: Path, output_file: Optional[Path] = None, batch_size: int = 50, error_limit: int = 1000):
        """Initialize the command.
        
        Args:
            config: Application configuration
            input_file: Path to input CSV file
            output_file: Optional path to save results
            batch_size: Number of records to process per batch
            error_limit: Maximum number of errors before stopping
        """
        super().__init__(config, input_file, output_file)
        self.batch_size = batch_size
        self.error_limit = error_limit
    
    @command_error_handler
    def execute(self) -> None:
        """Execute the command."""
        session_manager = SessionManager(self.config.database_url)
        
        try:
            self.logger.info(f"Processing sales receipts from {self.input_file}")
            self.logger.info(f"Batch size: {self.batch_size}, Error limit: {self.error_limit}")
            
            # Read CSV file
            self.logger.info("Reading CSV file...")
            if self.debug:
                self.logger.debug(f"Reading CSV with low_memory=False from {self.input_file}")
            df = pd.read_csv(self.input_file, low_memory=False)
            if self.debug:
                self.logger.debug("Normalizing dataframe columns")
            df = normalize_dataframe_columns(df)
            self.logger.info(f"Found {len(df)} rows")
            
            # Process companies first
            self.logger.info("")
            self.logger.info("=== Phase 1: Company Processing ===")
            if self.debug:
                self.logger.debug("Initializing CompanyProcessor")
            # Convert config to dict for CompanyProcessor
            config_dict = {
                'database_url': self.config.database_url,
                'batch_size': self.batch_size
            }
            company_processor = CompanyProcessor(config_dict, batch_size=self.batch_size)
            company_result = company_processor.process(df)
            if self.debug:
                self.logger.debug(f"Company processing stats: {company_processor.get_stats()}")
            
            # Process customers next
            self.logger.info("")
            self.logger.info("=== Phase 2: Customer Processing ===")
            if self.debug:
                self.logger.debug("Initializing ProcessReceiptCustomersCommand")
            from .receipt_customers import ProcessReceiptCustomersCommand
            customer_processor = ProcessReceiptCustomersCommand(self.config, self.input_file, None, self.batch_size, self.error_limit)
            customer_processor.execute()
            # The execute method handles the processing internally
            
            # Customer processing is handled by the execute method
            # If there were critical errors, execute would have raised an exception
            
            # Process receipts next
            self.logger.info("")
            self.logger.info("=== Phase 3: Receipt Processing ===")
            if self.debug:
                self.logger.debug(f"Initializing SalesReceiptProcessor with batch_size={self.batch_size}, error_limit={self.error_limit}")
            receipt_processor = SalesReceiptProcessor(session_manager, self.batch_size, self.error_limit)
            
            # Check for validation issues
            critical_issues, warnings = receipt_processor.validate_data(df)
            
            # Show all validation issues
            if warnings or critical_issues:
                self.logger.info("Validation Summary:")
                
                if critical_issues:
                    self.logger.error("Critical Issues (must fix):")
                    for issue in critical_issues:
                        self.logger.error(f"  - {issue}")
                    return
                
                if warnings:
                    self.logger.warning("Rows that will be skipped:")
                    for warning in warnings:
                        self.logger.warning(f"  - {warning}")
                    
                    # Calculate valid rows
                    skipped_rows = sum(int(w.split()[1]) for w in warnings if w.split()[1].isdigit())
                    valid_rows = len(df) - skipped_rows
                    self.logger.info(f"Will process {valid_rows} valid rows")
                    if self.debug:
                        self.logger.debug(f"Skipping {skipped_rows} rows due to validation warnings")
                    self.logger.info("Continuing with processing...")
            
            processed_df = receipt_processor.process(df)
            
            # Convert processor results to our standard format
            receipt_result = {
                'success': (
                    receipt_processor.stats['failed_batches'] == 0 and 
                    receipt_processor.stats['errors'] < self.error_limit and
                    receipt_processor.stats['created'] + receipt_processor.stats['updated'] > 0
                ),
                'summary': {
                    'stats': receipt_processor.stats
                }
            }
            
            if not receipt_result['success']:
                self.logger.error("Failed to process sales receipts")
                if receipt_processor.stats['failed_batches'] > 0:
                    self.logger.error(f"Failed batches: {receipt_processor.stats['failed_batches']}")
                if receipt_processor.stats['errors'] >= self.error_limit:
                    self.logger.error(f"Error limit reached: {receipt_processor.stats['errors']} errors")
                if receipt_processor.stats['created'] + receipt_processor.stats['updated'] == 0:
                    self.logger.error("No orders were created or updated")
                return
            
            self.logger.info("Sales receipt processing complete")
            self.logger.info(f"Created: {receipt_result['summary']['stats']['created']}")
            self.logger.info(f"Updated: {receipt_result['summary']['stats']['updated']}")
            self.logger.info(f"Errors: {receipt_result['summary']['stats']['errors']}")
            self.logger.info(f"Customer Lookup Failures: {receipt_result['summary']['stats']['customers_not_found']}")
            
            # Only proceed to products if we have orders
            if receipt_processor.stats['created'] + receipt_processor.stats['updated'] > 0:
                self.logger.info("")
                self.logger.info("=== Phase 4: Product Processing ===")
                if self.debug:
                    self.logger.debug("Initializing ProductProcessor")
                product_processor = ProductProcessor(session_manager, self.batch_size)
                product_processor.debug = self.debug
                
                # Process products
                product_result = product_processor.process_file(self.input_file)
                
                if not product_result['success']:
                    self.logger.error("Failed to process products")
                    if product_result['summary']['stats']['validation_errors'] > 0:
                        self.logger.error(f"Validation errors: {product_result['summary']['stats']['validation_errors']}")
                    if product_result['summary']['stats']['total_errors'] > 0:
                        self.logger.error(f"Processing errors: {product_result['summary']['stats']['total_errors']}")
                    return
                
                self.logger.info("Product processing complete")
                self.logger.info(f"Total products: {product_result['summary']['stats']['total_products']}")
                self.logger.info(f"Created: {product_result['summary']['stats']['created']}")
                self.logger.info(f"Updated: {product_result['summary']['stats']['updated']}")
                self.logger.info(f"Skipped: {product_result['summary']['stats']['skipped']}")
                if product_result['summary']['stats']['validation_errors'] > 0:
                    self.logger.warning(f"Validation errors: {product_result['summary']['stats']['validation_errors']}")
                
                self.logger.info("")
                self.logger.info("=== Phase 5: Line Item Processing ===")
                if self.debug:
                    self.logger.debug(f"Initializing SalesReceiptLineItemProcessor with batch_size={self.batch_size}, error_limit={self.error_limit}")
                line_item_processor = SalesReceiptLineItemProcessor(session_manager, self.batch_size, self.error_limit)
                
                # Check for validation issues
                critical_issues, warnings = line_item_processor.validate_data(df)
                
                # Show all validation issues
                if warnings or critical_issues:
                    self.logger.info("Line Item Validation Summary:")
                    
                    if critical_issues:
                        self.logger.error("Critical Issues (must fix):")
                        for issue in critical_issues:
                            self.logger.error(f"  - {issue}")
                        return
                    
                    if warnings:
                        self.logger.warning("Rows that will be skipped:")
                        for warning in warnings:
                            self.logger.warning(f"  - {warning}")
                        
                        # Calculate valid rows
                        skipped_rows = sum(int(w.split()[1]) for w in warnings if w.split()[1].isdigit())
                        valid_rows = len(df) - skipped_rows
                        self.logger.info(f"Will process {valid_rows} valid line items")
                        if self.debug:
                            self.logger.debug(f"Skipping {skipped_rows} line items due to validation warnings")
                        self.logger.info("Continuing with processing...")
                
                line_item_result = line_item_processor.process_file(df)  # Pass DataFrame directly
            else:
                self.logger.info("Skipping line item processing since no orders were created")
                return
            
            if not line_item_result['success']:
                self.logger.error("Failed to process line items")
                if 'validation_issues' in line_item_result['summary']:
                    self.logger.error("Validation issues:")
                    for issue in line_item_result['summary']['validation_issues']:
                        self.logger.error(f"  - {issue}")
                return
                
            self.logger.info("Line item processing complete")
            self.logger.info(f"Total line items: {line_item_result['summary']['stats']['total_line_items']}")
            self.logger.info(f"Orders processed: {line_item_result['summary']['stats']['orders_processed']}")
            self.logger.info(f"Products not found: {line_item_result['summary']['stats']['products_not_found']}")
            self.logger.info(f"Orders not found: {line_item_result['summary']['stats']['orders_not_found']}")
            
            # Save results if output file specified
            if self.output_file:
                if self.debug:
                    self.logger.debug(f"Saving results to {self.output_file}")
                results = {
                    'company_processing': {
                        'success': True,
                        'summary': {
                            'stats': company_processor.get_stats()
                        }
                    },
                    'receipt_processing': receipt_result,
                    'product_processing': product_result,
                    'line_item_processing': line_item_result
                }
                with open(self.output_file, 'w') as f:
                    json.dump(results, f, indent=2)
                self.logger.info(f"Detailed results saved to {self.output_file}")
                
        except Exception as e:
            self.logger.error(f"Failed to process file: {str(e)}", exc_info=self.debug)
            self.error_tracker.add_error(
                'PROCESS_RECEIPTS_ERROR',
                f"Failed to process sales receipts: {str(e)}",
                {
                    'input_file': str(self.input_file),
                    'batch_size': self.batch_size,
                    'error_limit': self.error_limit
                }
            )
            return

# Click command wrapper
@click.command()
@click.argument('file_path', type=click.Path(exists=True, path_type=Path))
@click.option('--output', type=click.Path(file_okay=True, dir_okay=False, path_type=Path), help='Save processing results to file')
@click.option('--batch-size', default=50, help='Number of records to process per batch')
@click.option('--error-limit', default=1000, help='Maximum number of errors before stopping')
@click.option('--debug', is_flag=True, help='Enable debug logging')
@click.pass_context
def process_receipts(ctx, file_path: Path, output: Optional[Path], batch_size: int, error_limit: int, debug: bool):
    """Import sales receipt data from a CSV file into the database."""
    config = ctx.obj.get('config')
    if not config:
        click.echo("Error: No configuration found in context")
        return
        
    command = ProcessReceiptsCommand(config, file_path, output, batch_size, error_limit)
    command.debug = debug
    command.execute()

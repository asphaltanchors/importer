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
from ...processors.line_item import LineItemProcessor
from ...processors.company import CompanyProcessor
from ...processors.product import ProductProcessor
from ...processors.error_tracker import ErrorTracker
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
        self.error_tracker = ErrorTracker()
    
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
            df = pd.read_csv(
                self.input_file,
                encoding='cp1252',
                dtype=str,  # Read all columns as strings to preserve IDs
                skipinitialspace=True
            )
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
            df = company_processor.process(df)
            company_stats = company_processor.get_stats()
            click.echo(f"Companies processed: {company_stats['total_processed']}")
            click.echo(f"Successful batches: {company_stats['successful_batches']}")
            click.echo(f"Failed batches: {company_stats['failed_batches']}")
            click.echo(f"Total errors: {company_stats['total_errors']}")
            
            # Process customers next
            self.logger.info("")
            self.logger.info("=== Phase 2: Customer Processing ===")
            if self.debug:
                self.logger.debug("Initializing ProcessReceiptCustomersCommand")
            from .receipt_customers import ProcessReceiptCustomersCommand
            customer_processor = ProcessReceiptCustomersCommand(self.config, self.input_file, None, self.batch_size, self.error_limit)
            customer_processor.execute()
            # The execute method handles the processing internally
            
            # Process receipts next
            self.logger.info("")
            self.logger.info("=== Phase 3: Receipt Processing ===")
            if self.debug:
                self.logger.debug(f"Initializing SalesReceiptProcessor with batch_size={self.batch_size}, error_limit={self.error_limit}")
            receipt_processor = SalesReceiptProcessor(
                {'database_url': self.config.database_url},
                batch_size=self.batch_size,
                error_limit=self.error_limit
            )
            
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
            receipt_stats = receipt_processor.get_stats()
            
            self.logger.info("Sales receipt processing complete")
            self.logger.info(f"Total processed: {receipt_stats['total_processed']}")
            self.logger.info(f"Successful batches: {receipt_stats['successful_batches']}")
            self.logger.info(f"Failed batches: {receipt_stats['failed_batches']}")
            self.logger.info(f"Total errors: {receipt_stats['total_errors']}")
            
            # Only proceed to products if we have successful receipts
            if receipt_stats['successful_batches'] > 0:
                self.logger.info("")
                self.logger.info("=== Phase 4: Product Processing ===")
                if self.debug:
                    self.logger.debug("Initializing ProductProcessor")
                product_processor = ProductProcessor(
                    {'database_url': self.config.database_url},
                    batch_size=self.batch_size
                )
                product_processor.debug = self.debug
                
                # Process products
                df = product_processor.process(df)
                product_stats = product_processor.get_stats()
                
                self.logger.info("Product processing complete")
                self.logger.info(f"Total processed: {product_stats['total_processed']}")
                self.logger.info(f"Successful batches: {product_stats['successful_batches']}")
                self.logger.info(f"Failed batches: {product_stats['failed_batches']}")
                self.logger.info(f"Total errors: {product_stats['total_errors']}")
                
                # Only proceed to line items if we have successful products
                if product_stats['successful_batches'] > 0:
                    self.logger.info("")
                    self.logger.info("=== Phase 5: Line Item Processing ===")
                    if self.debug:
                        self.logger.debug(f"Initializing LineItemProcessor")
                    line_item_processor = LineItemProcessor(
                        {'database_url': self.config.database_url},
                        batch_size=self.batch_size,
                        error_limit=self.error_limit,
                        debug=self.debug
                    )
                    
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
                    
                    df = line_item_processor.process(df)
                    line_item_stats = line_item_processor.get_stats()
                    
                    self.logger.info("Line item processing complete")
                    self.logger.info(f"Total processed: {line_item_stats['total_processed']}")
                    self.logger.info(f"Successful batches: {line_item_stats['successful_batches']}")
                    self.logger.info(f"Failed batches: {line_item_stats['failed_batches']}")
                    self.logger.info(f"Total errors: {line_item_stats['total_errors']}")
                else:
                    self.logger.info("Skipping line item processing due to product processing errors")
            else:
                self.logger.info("Skipping product and line item processing due to receipt processing errors")
            
            # Save results if output file specified
            if self.output_file:
                if self.debug:
                    self.logger.debug(f"Saving results to {self.output_file}")
                results = {
                    'company_stats': company_stats,
                    'receipt_stats': receipt_stats,
                    'product_stats': product_stats if 'product_stats' in locals() else None,
                    'line_item_stats': line_item_stats if 'line_item_stats' in locals() else None,
                    'processed_rows': len(df)
                }
                with open(self.output_file, 'w') as f:
                    json.dump(results, f, indent=2)
                self.logger.info(f"Detailed results saved to {self.output_file}")
                
        except Exception as e:
            # Log error but continue
            self.logger.error(f"Error during processing: {str(e)}")
            if self.debug:
                self.logger.debug("Error details:", exc_info=True)

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

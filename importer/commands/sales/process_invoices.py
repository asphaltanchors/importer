"""Process invoices from a CSV file.

A top-level command for importing invoice data into the database. This command:
1. Processes companies from invoice data
2. Creates/updates customer records
3. Creates/updates invoice records
4. Creates/updates product records
5. Processes line items with product mapping

The processing sequence ensures proper relationship handling:
- Companies must exist before customer records
- Customers must exist before invoice records
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
from ...processors.invoice import InvoiceProcessor
from ...processors.line_item import LineItemProcessor
from ...processors.company import CompanyProcessor
from ...processors.product import ProductProcessor
from ...db.session import SessionManager

class ProcessInvoicesCommand(FileInputCommand):
    """Command to process invoices from a sales data file."""
    
    def __init__(self, config, input_file: Path, output_file: Optional[Path] = None, batch_size: int = 100, error_limit: int = 1000):
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
        self.logger = get_logger(__name__)
    
    @command_error_handler
    def execute(self) -> None:
        """Execute the command."""
        session_manager = SessionManager(self.config.database_url)
        
        try:
            self.logger.info(f"Processing invoices from {self.input_file}")
            self.logger.info(f"Batch size: {self.batch_size}, Error limit: {self.error_limit}")
            
            # Convert config to dict format
            config_dict = {
                'database_url': self.config.database_url,
                'batch_size': self.batch_size
            }
            
            # Process invoices first
            with session_manager.get_session() as session:
                # Read CSV file
                self.logger.info("Reading CSV file...")
                if self.debug:
                    self.logger.debug(f"Reading CSV from {self.input_file}")
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

                # Phase 1: Company Processing
                self.logger.info("")
                self.logger.info("=== Phase 1: Company Processing ===")
                if self.debug:
                    self.logger.debug("Initializing CompanyProcessor")
                
                # Initialize company processor with error tracking
                company_processor = CompanyProcessor(config_dict, self.batch_size)
                company_processor.debug = self.debug
                
                # Validate data before processing
                critical_issues, warnings = company_processor.validate_data(df) if hasattr(company_processor, 'validate_data') else ([], [])
                
                # Show all validation issues
                if warnings or critical_issues:
                    self.logger.info("")
                    self.logger.info("Company Validation Summary:")
                    
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
                
                # Process companies
                processed_df = company_processor.process(df)
                
                # Get company processing results
                stats = company_processor.get_stats()
                company_result = {
                    'success': stats['total_errors'] < self.error_limit,
                    'summary': {
                        'stats': stats
                    }
                }

                if not company_result['success']:
                    self.logger.error("Failed to process companies - too many errors")
                    return

                self.logger.info("Company processing complete")
                self.logger.info(f"Domains extracted: {stats['domains_extracted']}")
                self.logger.info(f"Companies created: {stats['companies_created']}")
                self.logger.info(f"Rows with domain: {stats['rows_with_domain']}")
                if self.debug:
                    self.logger.debug(f"Errors: {stats['total_errors']}")

                # Phase 2: Invoice Processing
                self.logger.info("")
                self.logger.info("=== Phase 2: Invoice Processing ===")
                if self.debug:
                    self.logger.debug("Initializing InvoiceProcessor")
                invoice_processor = InvoiceProcessor(
                    config=config_dict,
                    batch_size=self.batch_size,
                    error_limit=self.error_limit,
                    debug=self.debug
                )
                processed_df = invoice_processor.process(df)
                
                # Get invoice processing results
                stats = invoice_processor.get_stats()
                invoice_result = {
                    'success': (
                        stats['failed_batches'] == 0 and
                        stats['total_errors'] < self.error_limit and
                        stats['created'] + stats['updated'] > 0
                    ),
                    'summary': {
                        'stats': stats
                    }
                }
                
                if not invoice_result['success']:
                    self.logger.error("Failed to process invoices")
                    if stats['failed_batches'] > 0:
                        self.logger.error(f"Failed batches: {stats['failed_batches']}")
                    if stats['total_errors'] >= self.error_limit:
                        self.logger.error(f"Error limit reached: {stats['total_errors']} errors")
                    if stats['created'] + stats['updated'] == 0:
                        self.logger.error("No invoices were created or updated")
                    return
                
                self.logger.info("Invoice processing complete")
                self.logger.info(f"Created: {stats['created']}")
                self.logger.info(f"Updated: {stats['updated']}")
                self.logger.info(f"Errors: {stats['total_errors']}")
            
            # Only proceed to products if we have orders
            if stats['created'] + stats['updated'] > 0:
                self.logger.info("")
                self.logger.info("=== Phase 3: Product Processing ===")
                if self.debug:
                    self.logger.debug("Initializing ProductProcessor")
                product_processor = ProductProcessor(
                    config=config_dict,
                    batch_size=self.batch_size,
                    error_limit=self.error_limit,
                    debug=self.debug
                )
                
                # Process products
                product_result = product_processor.process(df)
                
                # Get product processing results
                stats = product_processor.get_stats()
                product_result = {
                    'success': stats['total_errors'] < self.error_limit,
                    'summary': {
                        'stats': stats
                    }
                }
                
                if not product_result['success']:
                    self.logger.error("Failed to process products")
                    if stats['validation_errors'] > 0:
                        self.logger.error(f"Validation errors: {stats['validation_errors']}")
                    if stats['total_errors'] > 0:
                        self.logger.error(f"Processing errors: {stats['total_errors']}")
                    return
                
                self.logger.info("Product processing complete")
                self.logger.info(f"Total products: {stats['total_products']}")
                self.logger.info(f"Created: {stats['created']}")
                self.logger.info(f"Updated: {stats['updated']}")
                self.logger.info(f"Skipped: {stats['skipped']}")
                if stats['validation_errors'] > 0:
                    self.logger.warning(f"Validation errors: {stats['validation_errors']}")
                
                self.logger.info("")
                self.logger.info("=== Phase 4: Line Item Processing ===")
                if self.debug:
                    self.logger.debug("Initializing LineItemProcessor")
                line_item_processor = LineItemProcessor(
                    config=config_dict,
                    batch_size=self.batch_size,
                    error_limit=self.error_limit,
                    debug=self.debug
                )
                line_item_result = line_item_processor.process(df)
                
                # Get line item processing results
                stats = line_item_processor.get_stats()
                line_item_result = {
                    'success': stats['total_errors'] < self.error_limit,
                    'summary': {
                        'stats': stats
                    }
                }
                
                if not line_item_result['success']:
                    self.logger.error("Failed to process line items")
                    if stats['validation_errors'] > 0:
                        self.logger.error(f"Validation errors: {stats['validation_errors']}")
                    if stats['total_errors'] > 0:
                        self.logger.error(f"Processing errors: {stats['total_errors']}")
                    return
                    
                self.logger.info("Line item processing complete")
                self.logger.info(f"Total line items: {stats['total_line_items']}")
                self.logger.info(f"Orders processed: {stats['orders_processed']}")
                self.logger.info(f"Products not found: {stats['products_not_found']}")
                self.logger.info(f"Orders not found: {stats['orders_not_found']}")
            else:
                self.logger.info("Skipping line item processing since no invoices were created")
                return
            
            # Save results if output file specified
            if self.output_file:
                if self.debug:
                    self.logger.debug(f"Saving results to {self.output_file}")
                results = {
                    'company_processing': company_result,
                    'invoice_processing': invoice_result,
                    'product_processing': product_result,
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
@click.option('--error-limit', default=1000, help='Maximum number of errors before stopping')
@click.option('--debug', is_flag=True, help='Enable debug logging')
@click.pass_context
def process_invoices(ctx, file_path: Path, output: Optional[Path], batch_size: int, error_limit: int, debug: bool):
    """Import invoice data from a CSV file into the database."""
    config = ctx.obj.get('config')
    if not config:
        logger = get_logger(__name__)
        logger.error("No configuration found in context")
        return
        
    command = ProcessInvoicesCommand(config, file_path, output, batch_size, error_limit)
    command.debug = debug
    command.execute()

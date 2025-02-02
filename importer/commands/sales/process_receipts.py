"""Process sales receipts command."""

from pathlib import Path
import click
import json
import logging
from typing import Optional
import pandas as pd

from ...cli.base import FileInputCommand, command_error_handler
from ...utils.csv_normalization import normalize_dataframe_columns
from ...processors.sales_receipt import SalesReceiptProcessor
from ...processors.sales_receipt_line_item import SalesReceiptLineItemProcessor
from ...db.session import SessionManager

class ProcessReceiptsCommand(FileInputCommand):
    """Command to process sales receipts from a sales data file."""
    
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
    
    @command_error_handler
    def execute(self) -> None:
        """Execute the command."""
        session_manager = SessionManager(self.config.database_url)
        
        try:
            # Process receipts first
            with session_manager.get_session() as session:
                # Read CSV file
                df = pd.read_csv(self.input_file)
                df = normalize_dataframe_columns(df)
                
                receipt_processor = SalesReceiptProcessor(session, self.batch_size)
                processed_df = receipt_processor.process(df)
                
                # Convert processor results to our standard format
                receipt_result = {
                    'success': receipt_processor.stats['failed_batches'] == 0,
                    'summary': {
                        'stats': receipt_processor.stats
                    }
                }
                
                if not receipt_result['success']:
                    click.echo("Failed to process sales receipts")
                    return
                
                click.echo("Sales receipt processing complete")
                click.echo(f"Created: {receipt_result['summary']['stats']['created']}")
                click.echo(f"Updated: {receipt_result['summary']['stats']['updated']}")
                click.echo(f"Errors: {receipt_result['summary']['stats']['errors']}")
            
            # Then process line items
            line_item_processor = SalesReceiptLineItemProcessor(session_manager, self.batch_size)
            line_item_result = line_item_processor.process_file(str(self.input_file))
            
            if not line_item_result['success']:
                click.echo("Failed to process line items")
                return
                
            click.echo("Line item processing complete")
            click.echo(f"Total line items: {line_item_result['summary']['stats']['total_line_items']}")
            click.echo(f"Orders processed: {line_item_result['summary']['stats']['orders_processed']}")
            click.echo(f"Products not found: {line_item_result['summary']['stats']['products_not_found']}")
            click.echo(f"Orders not found: {line_item_result['summary']['stats']['orders_not_found']}")
            
            # Save results if output file specified
            if self.output_file:
                results = {
                    'receipt_processing': receipt_result,
                    'line_item_processing': line_item_result
                }
                with open(self.output_file, 'w') as f:
                    json.dump(results, f, indent=2)
                click.echo(f"\nDetailed results saved to {self.output_file}")
                
        except Exception as e:
            logging.error(f"Failed to process file: {str(e)}")
            click.echo(f"Error: {str(e)}")
            return

# Click command wrapper
@click.command()
@click.argument('file_path', type=click.Path(exists=True, path_type=Path))
@click.option('--output', type=click.Path(file_okay=True, dir_okay=False, path_type=Path), help='Save processing results to file')
@click.option('--batch-size', default=100, help='Number of records to process per batch')
@click.pass_context
def process_receipts(ctx, file_path: Path, output: Optional[Path], batch_size: int):
    """Process sales receipts from a CSV file."""
    config = ctx.obj.get('config')
    if not config:
        click.echo("Error: No configuration found in context")
        return
        
    command = ProcessReceiptsCommand(config, file_path, output, batch_size)
    command.execute()

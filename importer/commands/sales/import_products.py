"""Product import command for dedicated product CSV files."""

from pathlib import Path
from typing import Optional

import click

from ...cli.base import FileInputCommand
from ...cli.config import Config
from ...processors.product_import import ProductImportProcessor

class ImportProductsCommand(FileInputCommand):
    """Import products from a CSV file with cost and list price."""
    
    name = 'import-products'
    help = 'Import products from a CSV file with cost and list price'

    def __init__(
        self, 
        config: Config, 
        input_file: Path, 
        output_file: Optional[Path] = None, 
        batch_size: int = 100,
        track_history: bool = True,
        debug: bool = False
    ):
        """Initialize command.
        
        Args:
            config: Application configuration
            input_file: Path to input CSV file
            output_file: Optional path to save results
            batch_size: Number of products to process per batch
            track_history: Whether to track price history changes
            debug: Enable debug logging
        """
        super().__init__(config, input_file, output_file)
        self.batch_size = batch_size
        self.track_history = track_history
        self.debug = debug

    def execute(self) -> Optional[int]:
        """Execute the command.
        
        Returns:
            Optional exit code
        """
        processor = ProductImportProcessor(
            config={'database_url': self.config.database_url},
            batch_size=self.batch_size,
            debug=self.debug,
            track_price_history=self.track_history
        )
        
        self.logger.info(f"Importing products from {self.input_file}")
        self.logger.info(f"Batch size: {self.batch_size}")
        self.logger.info(f"Price history tracking: {'Enabled' if self.track_history else 'Disabled'}")
        
        results = processor.process_file(self.input_file)
        
        # Print final summary
        stats = results['summary']['stats']
        self.logger.info("\nProcessing complete:")
        self.logger.info(f"Total products: {stats['total_products']}")
        self.logger.info(f"Created: {stats['created']}")
        self.logger.info(f"Updated: {stats['updated']}")
        self.logger.info(f"Price updates: {stats['price_updated']}")
        if self.track_history:
            self.logger.info(f"Price history entries: {stats['history_entries']}")
        self.logger.info(f"Skipped: {stats['skipped']}")
        self.logger.info(f"Successful batches: {stats['successful_batches']}")
        
        if stats['failed_batches'] > 0:
            self.logger.error(f"Failed batches: {stats['failed_batches']}")
            self.logger.error(f"Total errors: {stats['total_errors']}")
            return 1
            
        # Save results if output file specified
        if self.output_file:
            self.save_results(results)
            self.logger.info(f"\nDetailed results saved to {self.output_file}")
        
        return 0

@click.command('import-products')
@click.argument('file', type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path))
@click.option('--output', type=click.Path(file_okay=True, dir_okay=False, path_type=Path), 
              help='Save processing results to file')
@click.option('--batch-size', type=int, default=100, help='Number of records to process per batch')
@click.option('--track-history/--no-track-history', default=True, help='Track price history changes')
@click.pass_context
def import_products(ctx, file: Path, output: Path, batch_size: int, track_history: bool):
    """Import products from a CSV file with cost and list price."""
    try:
        config = Config.from_env()
        debug = ctx.obj.get('debug', False) if ctx.obj else False
        command = ImportProductsCommand(
            config, 
            file, 
            output, 
            batch_size, 
            track_history,
            debug
        )
        command.execute()
    except Exception as e:
        click.secho(f"Error: {str(e)}", fg='red')
        raise click.Abort()

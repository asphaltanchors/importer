"""Product processing command for sales data."""

from pathlib import Path
from typing import Optional

from ...cli.base import FileInputCommand
from ...cli.config import Config
from ...processors.product import ProductProcessor
from ...db.session import SessionManager

class ProcessProductsCommand(FileInputCommand):
    """Process products from a sales data file."""
    
    name = 'process-products'
    help = 'Process products from a sales data file'

    def __init__(self, config: Config, input_file: Path, output_file: Optional[Path] = None, batch_size: int = 100):
        """Initialize command.
        
        Args:
            config: Application configuration
            input_file: Path to input CSV file
            output_file: Optional path to save results
            batch_size: Number of products to process per batch
        """
        super().__init__(config, input_file, output_file)
        self.batch_size = batch_size

    def execute(self) -> Optional[int]:
        """Execute the command.
        
        Returns:
            Optional exit code
        """
        session_manager = SessionManager(self.config.database_url)
        processor = ProductProcessor(session_manager, self.batch_size)
        
        self.logger.info(f"Processing products from {self.input_file}")
        self.logger.info(f"Batch size: {self.batch_size}")
        
        results = processor.process_file(self.input_file)
        
        # Print final summary
        stats = results['summary']['stats']
        self.logger.info("\nProcessing complete:")
        self.logger.info(f"Total products: {stats['total_products']}")
        self.logger.info(f"Created: {stats['created']}")
        self.logger.info(f"Updated: {stats['updated']}")
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

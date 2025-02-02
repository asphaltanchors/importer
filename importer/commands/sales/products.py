"""Product processing command for sales data."""

from pathlib import Path
from typing import Optional

from ...cli.base import FileInputCommand
from ...cli.config import Config
from ...processors.product import ProductProcessor

class ProcessProductsCommand(FileInputCommand):
    """Process products from a sales data file."""
    
    name = 'process-products'
    help = 'Process products from a sales data file'

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
        processor = ProductProcessor(self.config.database_url)
        results = processor.process(self.input_file)
        
        # Print summary
        stats = results['summary']['stats']
        self.logger.info(f"Processed {stats['total_products']} products:")
        self.logger.info(f"  Created: {stats['created']}")
        self.logger.info(f"  Updated: {stats['updated']}")
        self.logger.info(f"  Skipped: {stats['skipped']}")
        
        if results['summary']['errors']:
            self.logger.warning("Errors encountered:")
            for error in results['summary']['errors']:
                self.logger.warning(f"  {error['message']}")
            return 1
            
        return 0

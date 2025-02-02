"""Order processing command for sales data."""

from pathlib import Path
from typing import Optional, Dict, Any
import json
import logging

from ...cli.base import FileInputCommand
from ...cli.config import Config
from ...db.session import SessionManager
from ...processors.order import OrderProcessor

class ProcessOrdersCommand(FileInputCommand):
    """Process orders from a sales data file."""
    
    name = 'process-orders'
    help = 'Process orders from a sales data file'

    def __init__(self, config: Config, input_file: Path, output_file: Optional[Path] = None, batch_size: int = 100):
        """Initialize command.
        
        Args:
            config: Application configuration
            input_file: Path to input CSV file
            output_file: Optional path to save results
            batch_size: Number of orders to process per batch
        """
        super().__init__(config, input_file, output_file)
        self.batch_size = batch_size
        self.session_manager = SessionManager(config.database_url)

    def execute(self) -> Optional[int]:
        """Execute the command.
        
        Returns:
            Optional exit code
        """
        try:
            # Determine if this is a sales receipt file by checking first line
            with open(self.input_file, 'r') as f:
                header = f.readline()
                is_sales_receipt = 'Sales Receipt No' in header
            
            self.logger.info(f"Processing orders from {self.input_file}")
            self.logger.info(f"File type: {'Sales Receipt' if is_sales_receipt else 'Invoice'}")
            self.logger.info(f"Batch size: {self.batch_size}")
            
            # Process orders
            processor = OrderProcessor(self.session_manager, self.batch_size)
            results = processor.process_file(self.input_file, is_sales_receipt)
            
            # Print final summary
            stats = results['summary']['stats']
            self.logger.info("\nProcessing complete:")
            self.logger.info(f"Total orders: {stats['total_orders']}")
            self.logger.info(f"Created: {stats['created']}")
            self.logger.info(f"Updated: {stats['updated']}")
            self.logger.info(f"Successful batches: {stats['successful_batches']}")
            
            if stats['failed_batches'] > 0:
                self.logger.error(f"Failed batches: {stats['failed_batches']}")
                self.logger.error(f"Total errors: {stats['total_errors']}")
            
            if stats['customers_not_found'] > 0:
                self.logger.warning(f"Customers not found: {stats['customers_not_found']}")
            
            if stats['invalid_addresses'] > 0:
                self.logger.warning(f"Invalid addresses: {stats['invalid_addresses']}")
            
            # Save results if output file specified
            if self.output_file:
                with open(self.output_file, 'w') as f:
                    json.dump(results, f, indent=2)
                self.logger.info(f"\nDetailed results saved to {self.output_file}")
            
            # Return success if no failed batches
            return 1 if stats['failed_batches'] > 0 else 0
            
        except Exception as e:
            self.logger.error(f"Failed to process file: {str(e)}")
            return 1

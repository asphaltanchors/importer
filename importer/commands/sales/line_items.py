"""Line item processing command for sales data."""

from pathlib import Path
from typing import Optional, Dict, Any, List
import json
import logging

from ...cli.base import FileInputCommand
from ...cli.config import Config
from ...db.session import SessionManager
from ...processors.line_item import LineItemProcessor

class ProcessLineItemsCommand(FileInputCommand):
    """Process line items from a sales data file."""
    
    name = 'process-line-items'
    help = 'Process line items from a sales data file'

    def __init__(self, config: Config, input_file: Path, output_file: Optional[Path] = None, 
                 batch_size: int = 100):
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
            self.logger.info(f"Processing line items from {self.input_file}")
            self.logger.info(f"Batch size: {self.batch_size}")
            
            # Process line items
            processor = LineItemProcessor(self.session_manager, self.batch_size)
            results = processor.process_file(self.input_file)
            
            # Print final summary
            stats = results['summary']['stats']
            self.logger.info("\nProcessing complete:")
            self.logger.info(f"Total line items: {stats['total_line_items']}")
            self.logger.info(f"Orders processed: {stats['orders_processed']}")
            self.logger.info(f"Successful batches: {stats['successful_batches']}")
            
            if stats['failed_batches'] > 0:
                self.logger.error(f"Failed batches: {stats['failed_batches']}")
                self.logger.error(f"Total errors: {stats['total_errors']}")
            
            if stats['products_not_found'] > 0:
                self.logger.warning(f"Products not found: {stats['products_not_found']}")
            
            if stats['orders_not_found'] > 0:
                self.logger.warning(f"Orders not found: {stats['orders_not_found']}")
            
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

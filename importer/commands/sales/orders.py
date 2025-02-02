"""Order processing command for sales data."""

from pathlib import Path
from typing import Optional
import json
import logging
import pandas as pd

from ...cli.base import FileInputCommand
from ...cli.config import Config
from ...processors.invoice import InvoiceProcessor
from ...utils.csv_normalization import normalize_dataframe_columns

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
        self.logger.debug("Initializing ProcessOrdersCommand")

    def execute(self) -> Optional[int]:
        """Execute the command.
        
        Returns:
            Optional exit code
        """
        try:
            self.logger.info(f"Processing orders from {self.input_file}")
            
            # Read and normalize CSV
            df = pd.read_csv(self.input_file)
            df = normalize_dataframe_columns(df)
            
            # Process orders using session
            session = self.get_session()
            try:
                processor = InvoiceProcessor(session, self.batch_size)
                processed_df = processor.process(df)
                
                # Get statistics
                stats = processor.get_stats()
                
                # Print final summary
                self.logger.info("\nProcessing complete:")
                self.logger.info(f"Total orders: {stats['total_invoices']}")
                self.logger.info(f"Created: {stats['created']}")
                self.logger.info(f"Updated: {stats['updated']}")
                
                if stats['errors'] > 0:
                    self.logger.error(f"Total errors: {stats['errors']}")
                
                # Save results if output file specified
                if self.output_file:
                    results = {
                        'stats': stats,
                        'processed_rows': len(processed_df),
                        'order_ids': processed_df['order_id'].dropna().tolist()
                    }
                    with open(self.output_file, 'w') as f:
                        json.dump(results, f, indent=2)
                    self.logger.info(f"\nDetailed results saved to {self.output_file}")
                
                # Return success if no errors
                return 1 if stats['errors'] > 0 else 0
                
            finally:
                session.close()
            
        except Exception as e:
            self.logger.error(f"Failed to process file: {str(e)}")
            return 1

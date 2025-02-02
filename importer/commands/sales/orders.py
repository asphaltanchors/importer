"""Order processing command for sales data."""

from pathlib import Path
from typing import Optional, Dict, Any
import csv
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

    def __init__(self, config: Config, input_file: Path, output_file: Optional[Path] = None):
        """Initialize command.
        
        Args:
            config: Application configuration
            input_file: Path to input CSV file
            output_file: Optional path to save results
        """
        super().__init__(config, input_file, output_file)
        self.session_manager = SessionManager(config.database_url)

    def execute(self) -> Optional[int]:
        """Execute the command.
        
        Returns:
            Optional exit code
        """
        results = {
            'success': True,
            'summary': {
                'stats': {
                    'total_orders': 0,
                    'created': 0,
                    'updated': 0,
                    'errors': 0
                },
                'errors': []
            }
        }
        
        try:
            with open(self.input_file, 'r') as f:
                reader = csv.DictReader(f)
                
                # Determine if this is a sales receipt file
                is_sales_receipt = 'Sales Receipt No' in reader.fieldnames
                
                # Group rows by invoice number
                invoices = {}
                for row in reader:
                    # Try both invoice and sales receipt number fields
                    invoice_number = (
                        row.get('Invoice No', '').strip() or 
                        row.get('Sales Receipt No', '').strip()
                    )
                    if invoice_number:
                        if invoice_number not in invoices:
                            invoices[invoice_number] = []
                        invoices[invoice_number].append(row)
                
                # Process each invoice
                with self.session_manager as session:
                    processor = OrderProcessor(session)
                    
                    for invoice_number, invoice_rows in invoices.items():
                        # Use first row for order header info
                        row = invoice_rows[0]
                        
                        # Process order
                        result = processor.process_order(row, is_sales_receipt)
                        
                        if result['success'] and result['order']:
                            # Add/update order in session
                            if not hasattr(result['order'], 'id'):
                                session.add(result['order'])
                                results['summary']['stats']['created'] += 1
                            else:
                                results['summary']['stats']['updated'] += 1
                        elif result['error']:
                            results['summary']['stats']['errors'] += 1
                            results['summary']['errors'].append({
                                'invoice': invoice_number,
                                **result['error']
                            })
                    
                    # Commit all changes
                    session.commit()
                    
                    results['summary']['stats']['total_orders'] = len(invoices)
                
                # Print summary
                self.logger.info(f"Processed {results['summary']['stats']['total_orders']} orders:")
                self.logger.info(f"  Created: {results['summary']['stats']['created']}")
                self.logger.info(f"  Updated: {results['summary']['stats']['updated']}")
                if results['summary']['stats']['errors']:
                    self.logger.warning(f"  Errors: {results['summary']['stats']['errors']}")
                    for error in results['summary']['errors']:
                        self.logger.warning(f"    Invoice {error['invoice']}: {error['message']}")
                
                # Save results if output file specified
                if self.output_file:
                    with open(self.output_file, 'w') as f:
                        json.dump(results, f, indent=2)
                
                return 1 if results['summary']['stats']['errors'] > 0 else 0
                
        except Exception as e:
            self.logger.error(f"Failed to process file: {str(e)}")
            return 1

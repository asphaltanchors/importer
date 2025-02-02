"""Payment processing command for sales data."""

from pathlib import Path
from typing import Optional, Dict, Any
import csv
import json
import logging

from ...cli.base import FileInputCommand
from ...cli.config import Config
from ...db.session import SessionManager
from ...processors.payment import PaymentProcessor

class ProcessPaymentsCommand(FileInputCommand):
    """Process payments from a sales data file."""
    
    name = 'process-payments'
    help = 'Process payments from a sales data file'

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
                    'total_payments': 0,
                    'processed': 0,
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
                
                # Process each invoice's payment info
                with self.session_manager as session:
                    processor = PaymentProcessor(session)
                    
                    for invoice_number, invoice_rows in invoices.items():
                        # Use first row for payment info
                        row = invoice_rows[0]
                        
                        # Process payment
                        result = processor.process_payment(row, is_sales_receipt)
                        
                        if result['success'] and result['order']:
                            results['summary']['stats']['processed'] += 1
                        elif result['error']:
                            results['summary']['stats']['errors'] += 1
                            results['summary']['errors'].append({
                                'invoice': invoice_number,
                                **result['error']
                            })
                    
                    # Commit all changes
                    session.commit()
                    
                    results['summary']['stats']['total_payments'] = len(invoices)
                
                # Print summary
                self.logger.info(f"Processed {results['summary']['stats']['total_payments']} payments:")
                self.logger.info(f"  Successful: {results['summary']['stats']['processed']}")
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

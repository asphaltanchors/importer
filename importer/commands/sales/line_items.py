"""Line item processing command for sales data."""

from pathlib import Path
from typing import Optional, Dict, Any, List
import csv
import logging

from ...cli.base import FileInputCommand
from ...cli.config import Config
from ...db.session import SessionManager
from ...processors.line_item import LineItemProcessor
from ...utils import generate_uuid

class ProcessLineItemsCommand(FileInputCommand):
    """Process line items from a sales data file."""
    
    name = 'process-line-items'
    help = 'Process line items from a sales data file'

    def __init__(self, config: Config, input_file: Path, output_file: Optional[Path] = None):
        """Initialize command.
        
        Args:
            config: Application configuration
            input_file: Path to input CSV file
            output_file: Optional path to save results
        """
        super().__init__(config, input_file, output_file)
        self.session_manager = SessionManager(config.database_url)

    def process_invoice_rows(self, rows: List[Dict[str, str]], invoice_number: str) -> Dict[str, Any]:
        """Process all line items for a single invoice.
        
        Args:
            rows: List of CSV rows for this invoice
            invoice_number: Invoice number for reference
            
        Returns:
            Dict containing processing results
        """
        results = {
            'success': True,
            'line_items': [],
            'totals': {
                'subtotal': 0.0,
                'tax_amount': 0.0
            },
            'errors': []
        }
        
        with self.session_manager as session:
            processor = LineItemProcessor(session)
            
            # Process each line item
            for row in rows:
                # Generate a temporary order ID since we don't have real orders yet
                temp_order_id = generate_uuid()
                
                result = processor.process_row(row, temp_order_id)
                if result['success'] and result['line_item']:
                    results['line_items'].append({
                        'order_id': temp_order_id,
                        'product_code': result['line_item'].productCode,
                        'quantity': result['line_item'].quantity,
                        'unit_price': result['line_item'].unitPrice,
                        'amount': result['line_item'].amount,
                        'service_date': result['line_item'].serviceDate,
                        'source_data': result['line_item'].sourceData
                    })
                elif result['error']:
                    results['errors'].append(result['error'])
            
            # Calculate totals
            if results['line_items']:
                # Convert line item dicts to OrderItem objects for total calculation
                from ...db.models import OrderItem
                order_items = [
                    OrderItem(
                        id=generate_uuid(),
                        orderId=item['order_id'],
                        productCode=item['product_code'],
                        quantity=item['quantity'],
                        unitPrice=item['unit_price'],
                        amount=item['amount'],
                        serviceDate=item['service_date'],
                        sourceData=item['source_data']
                    )
                    for item in results['line_items']
                ]
                results['totals'] = processor.calculate_totals(order_items)
        
        return results

    def execute(self) -> Optional[int]:
        """Execute the command.
        
        Returns:
            Optional exit code
        """
        results = {
            'success': True,
            'summary': {
                'stats': {
                    'total_invoices': 0,
                    'total_line_items': 0,
                    'errors': 0
                },
                'errors': []
            }
        }
        
        try:
            with open(self.input_file, 'r') as f:
                reader = csv.DictReader(f)
                
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
                
                # Process each invoice's line items
                for invoice_number, invoice_rows in invoices.items():
                    invoice_results = self.process_invoice_rows(invoice_rows, invoice_number)
                    
                    # Update statistics
                    results['summary']['stats']['total_line_items'] += len(invoice_results['line_items'])
                    if invoice_results['errors']:
                        results['summary']['stats']['errors'] += len(invoice_results['errors'])
                        results['summary']['errors'].extend([
                            {
                                'invoice': invoice_number,
                                **error
                            }
                            for error in invoice_results['errors']
                        ])
                
                results['summary']['stats']['total_invoices'] = len(invoices)
                
                # Print summary
                self.logger.info(f"Processed {results['summary']['stats']['total_invoices']} invoices:")
                self.logger.info(f"  Line items: {results['summary']['stats']['total_line_items']}")
                if results['summary']['stats']['errors']:
                    self.logger.warning(f"  Errors: {results['summary']['stats']['errors']}")
                    for error in results['summary']['errors']:
                        self.logger.warning(f"    Invoice {error['invoice']}: {error['message']}")
                
                # Save results if output file specified
                if self.output_file:
                    self.save_results(results)
                
                return 1 if results['summary']['stats']['errors'] > 0 else 0
                
        except Exception as e:
            self.logger.error(f"Failed to process file: {str(e)}")
            return 1

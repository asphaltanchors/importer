"""Line item processor for sales data."""

from datetime import datetime
from typing import Dict, Any, List, Set
from pathlib import Path
import logging
import pandas as pd
from sqlalchemy.orm import Session

from ..db.models import Product, OrderItem, Order
from ..utils import generate_uuid
from ..utils.product_mapping import map_product_code
from .base import BaseProcessor

class LineItemProcessor(BaseProcessor):
    """Process line items from sales data."""
    
    def __init__(self, session_manager, batch_size: int = 100):
        """Initialize the processor.
        
        Args:
            session_manager: Database session manager
            batch_size: Number of orders to process per batch
        """
        self.session_manager = session_manager
        super().__init__(None, batch_size)  # We'll manage sessions ourselves
        
        # Track processed items
        self.processed_orders: Set[str] = set()
        
        # Additional stats
        self.stats.update({
            'total_line_items': 0,
            'orders_processed': 0,
            'products_not_found': 0,
            'orders_not_found': 0
        })
        
    def process_file(self, file_path: Path, order_ids: List[str]) -> Dict[str, Any]:
        """Process line items from a CSV file.
        
        Args:
            file_path: Path to CSV file
            order_ids: List of valid order IDs to process line items for
            
        Returns:
            Dict containing processing results
        """
        try:
            # Read CSV into DataFrame
            df = pd.read_csv(file_path)
            
            # Group by invoice number
            invoice_groups = df.groupby(['Invoice No', 'Sales Receipt No'].dropna().iloc[0])
            total_invoices = len(invoice_groups)
            
            print(f"\nProcessing line items for {total_invoices} invoices in batches of {self.batch_size}", flush=True)
            
            # Process in batches
            current_batch = []
            batch_num = 1
            
            for invoice_number, invoice_df in invoice_groups:
                if invoice_number in self.processed_orders:
                    continue
                    
                current_batch.append((invoice_number, invoice_df))
                
                if len(current_batch) >= self.batch_size:
                    self._process_batch(current_batch, order_ids)
                    print(f"Batch {batch_num} complete ({len(current_batch)} invoices)", flush=True)
                    current_batch = []
                    batch_num += 1
            
            # Process final batch if any
            if current_batch:
                self._process_batch(current_batch, order_ids)
                print(f"Final batch complete ({len(current_batch)} invoices)", flush=True)
            
            return {
                'success': self.stats['failed_batches'] == 0,
                'summary': {
                    'stats': self.stats
                }
            }
            
        except Exception as e:
            logging.error(f"Failed to process file: {str(e)}")
            return {
                'success': False,
                'summary': {
                    'stats': self.stats
                }
            }
    
    def _process_batch(self, batch: List[tuple[str, pd.DataFrame]], order_ids: List[str]) -> None:
        """Process a batch of line items.
        
        Args:
            batch: List of (invoice_number, invoice_df) tuples
            order_ids: List of valid order IDs
        """
        try:
            with self.session_manager as session:
                for invoice_number, invoice_df in batch:
                    try:
                        # Find order
                        order = session.query(Order).filter(
                            Order.orderNumber == invoice_number,
                            Order.id.in_(order_ids)
                        ).first()
                        
                        if not order:
                            self.stats['orders_not_found'] += 1
                            logging.error(f"Order not found for invoice {invoice_number}")
                            continue
                        
                        # Process each line item
                        line_items = []
                        for _, row in invoice_df.iterrows():
                            result = self._process_line_item(row, order.id, session)
                            if result['success'] and result['line_item']:
                                line_items.append(result['line_item'])
                                self.stats['total_line_items'] += 1
                        
                        if line_items:
                            # Calculate and update order totals
                            totals = self._calculate_totals(line_items)
                            order.subtotal = totals['subtotal']
                            order.taxAmount = totals['tax_amount']
                            order.totalAmount = totals['subtotal'] + totals['tax_amount'] + totals['shipping_amount']
                            
                            self.processed_orders.add(invoice_number)
                            self.stats['orders_processed'] += 1
                            
                    except Exception as e:
                        logging.error(f"Error processing invoice {invoice_number}: {str(e)}")
                        continue
                
                # Commit batch
                session.commit()
                self.stats['successful_batches'] += 1
                
        except Exception as e:
            self.stats['failed_batches'] += 1
            self.stats['total_errors'] += 1
            logging.error(f"Error processing batch: {str(e)}")
    
    def _process_line_item(self, row: pd.Series, order_id: str, session: Session) -> Dict[str, Any]:
        """Process a single line item."""
        result = {
            'success': True,
            'line_item': None,
            'error': None
        }
        
        try:
            product_code = row.get('Product/Service', '').strip()
            if not product_code:
                return result  # Skip empty rows
            
            # Map product code using common utility
            mapped_code = map_product_code(
                product_code,
                row.get('Product/Service Description', '')
            )
            
            # Look up product
            product = session.query(Product).filter(
                Product.productCode == mapped_code
            ).first()
            
            if not product:
                self.stats['products_not_found'] += 1
                result['success'] = False
                result['error'] = {
                    'severity': 'ERROR',
                    'message': f"Product not found: {product_code}"
                }
                return result
            
            # Parse quantity and amount
            quantity = float(row.get('Product/Service Quantity', '1').strip() or '1')
            amount = 0.0
            amount_str = row.get('Product/Service Amount', '0').strip()
            if amount_str:
                try:
                    amount = float(amount_str.replace('$', '').replace(',', ''))
                except ValueError:
                    result['success'] = False
                    result['error'] = {
                        'severity': 'ERROR',
                        'message': f"Invalid amount for {product_code}"
                    }
                    return result
            
            unit_price = amount / quantity if quantity != 0 else 0
            
            # Parse service date if present
            service_date = None
            service_date_str = row.get('Service Date', '').strip()
            if service_date_str:
                try:
                    service_date = datetime.strptime(service_date_str, '%m-%d-%Y')
                except ValueError:
                    logging.warning(f"Invalid service date format for {product_code}")
            
            # Create line item
            line_item = OrderItem(
                id=generate_uuid(),
                orderId=order_id,
                productCode=product.productCode,
                description=row.get('Product/Service Description', '').strip(),
                quantity=quantity,
                unitPrice=unit_price,
                amount=amount,
                serviceDate=service_date,
                sourceData=row.to_dict()
            )
            
            session.add(line_item)
            result['line_item'] = line_item
            return result
            
        except Exception as e:
            result['success'] = False
            result['error'] = {
                'severity': 'ERROR',
                'message': f"Failed to process line item: {str(e)}"
            }
            return result
    
    def _calculate_totals(self, line_items: List[OrderItem]) -> Dict[str, float]:
        """Calculate order totals from line items."""
        subtotal = 0.0
        tax_amount = 0.0
        shipping_amount = 0.0
        
        # Define product categories
        tax_codes = ['SYS-TAX', 'SYS-NJ-TAX']
        shipping_codes = ['SYS-SHIPPING', 'SYS-HANDLING']
        
        for item in line_items:
            if item.productCode in tax_codes:
                tax_amount += item.amount
            elif item.productCode in shipping_codes:
                shipping_amount += item.amount
            else:
                subtotal += item.amount
        
        return {
            'subtotal': subtotal,
            'tax_amount': tax_amount,
            'shipping_amount': shipping_amount
        }

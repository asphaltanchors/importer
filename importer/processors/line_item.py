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
from ..utils.system_products import is_tax_product, is_shipping_product
from ..utils.csv_normalization import (
    normalize_dataframe_columns, 
    validate_required_columns,
    validate_json_data
)
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
        
        # Field mappings from CSV to our schema
        self.field_mappings = {
            'product_code': ['Product/Service'],
            'description': ['Product/Service Description'],
            'quantity': ['Product/Service Quantity'],
            'amount': ['Product/Service Amount'],
            'service_date': ['Service Date']
        }
        
        # Additional stats
        self.stats.update({
            'total_line_items': 0,
            'orders_processed': 0,
            'products_not_found': 0,
            'orders_not_found': 0,
            'skipped_items': 0
        })
        
    def process_file(self, file_path: Path) -> Dict[str, Any]:
        """Process line items from a CSV file.
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            Dict containing processing results
        """
        try:
            # Read CSV into DataFrame and normalize column names
            df = pd.read_csv(file_path)
            df = normalize_dataframe_columns(df)
            
            # Validate required columns
            required_columns = ['Product/Service']
            if not validate_required_columns(df, required_columns):
                raise ValueError("Missing required columns in CSV file")
            
            # Determine if this is a sales receipt file
            is_sales_receipt = 'Sales Receipt No' in df.columns
            group_by_col = 'Sales Receipt No' if is_sales_receipt else 'Invoice No'
            
            # Log column names for debugging
            self.logger.info(f"CSV columns: {df.columns.tolist()}")
            
            # Convert invoice numbers to strings to match order lookup
            df[group_by_col] = df[group_by_col].astype(str).str.strip()
            
            # Group by invoice/receipt number
            invoice_groups = df.groupby(group_by_col)
            total_invoices = len(invoice_groups)
            self.logger.debug(f"Found {total_invoices} unique invoice numbers")
            
            self.logger.info(f"\nProcessing line items for {total_invoices} invoices in batches of {self.batch_size}")
            
            # Process in batches
            current_batch = []
            batch_num = 1
            
            for invoice_number, invoice_df in invoice_groups:
                if invoice_number in self.processed_orders:
                    continue
                    
                current_batch.append((invoice_number, invoice_df))
                
                if len(current_batch) >= self.batch_size:
                    self._process_batch(current_batch)
                    print(f"Batch {batch_num} complete ({len(current_batch)} invoices)", flush=True)
                    current_batch = []
                    batch_num += 1
            
            # Process final batch if any
            if current_batch:
                self._process_batch(current_batch)
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
    
    def _clear_existing_line_items(self, order_id: str, session: Session) -> None:
        """Remove existing line items for an order before processing new ones.
        
        Args:
            order_id: Order ID to clear items for
            session: Database session
        """
        self.logger.debug(f"Clearing existing line items for order {order_id}")
        session.query(OrderItem).filter(
            OrderItem.orderId == order_id
        ).delete()

    def _process_batch(self, batch: List[tuple[str, pd.DataFrame]]) -> None:
        """Process a batch of line items.
        
        Args:
            batch: List of (invoice_number, invoice_df) tuples
        """
        try:
            with self.session_manager as session:
                for invoice_number, invoice_df in batch:
                    try:
                        # Find order
                        self.logger.debug(f"Looking for order {invoice_number}")
                        order = session.query(Order).filter(
                            Order.orderNumber == str(invoice_number)  # Convert to string in case invoice_number is numeric
                        ).first()
                        
                        if not order:
                            self.stats['orders_not_found'] += 1
                            self.logger.error(f"Order not found for invoice {invoice_number}")
                            continue
                        
                        self.logger.debug(f"Found order {order.id} for invoice {invoice_number}")
                        
                        # Clear existing line items before processing new ones
                        self._clear_existing_line_items(order.id, session)
                        
                        # Process each line item
                        line_items = []
                        for _, row in invoice_df.iterrows():
                            result = self._process_line_item(row, order.id, session)
                            if result['success'] and result['line_item']:
                                line_items.append(result['line_item'])
                                self.stats['total_line_items'] += 1
                        
                        if line_items:
                            # Calculate totals using all line items for the order
                            all_line_items = session.query(OrderItem).filter(
                                OrderItem.orderId == order.id
                            ).all()
                            totals = self._calculate_totals(all_line_items)
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
            product_code = self.get_mapped_field(row, 'product_code')
            if not product_code:
                return result  # Skip empty rows
            
            # Map product code using common utility
            description = self.get_mapped_field(row, 'description')
            mapped_code = map_product_code(product_code, description)
            self.logger.debug(f"Mapped product code '{product_code}' with description '{description}' to '{mapped_code}'")
            
            # Look up product
            product = session.query(Product).filter(
                Product.productCode == mapped_code
            ).first()
            
            if product:
                self.logger.debug(f"Found product {product.productCode}")
            
            if not product:
                self.stats['products_not_found'] += 1
                result['success'] = False
                result['error'] = {
                    'severity': 'ERROR',
                    'message': f"Product not found: {product_code}"
                }
                return result
            
            # Parse quantity and amount
            # Parse quantity and amount
            quantity = 1.0
            quantity_str = self.get_mapped_field(row, 'quantity')
            if quantity_str:
                try:
                    quantity = float(quantity_str.strip() or '1')
                except ValueError:
                    self.logger.warning(f"Invalid quantity for {product_code}: {quantity_str}")
                    quantity = 1.0

            amount = 0.0
            amount_str = self.get_mapped_field(row, 'amount')
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
            service_date_str = self.get_mapped_field(row, 'service_date')
            if service_date_str:
                try:
                    service_date = datetime.strptime(service_date_str, '%m-%d-%Y')
                except ValueError:
                    logging.warning(f"Invalid service date format for {product_code}")
            
            # Normalize source data for JSON serialization
            source_data = validate_json_data(row.to_dict())
            
            # Create line item
            line_item = OrderItem(
                id=generate_uuid(),
                orderId=order_id,
                productCode=product.productCode,
                description=description or '',
                quantity=quantity,
                unitPrice=unit_price,
                amount=amount,
                serviceDate=service_date,
                sourceData=source_data
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
    
    def get_mapped_field(self, row: pd.Series, field: str) -> str:
        """Get value for a mapped field from the row.
        
        Args:
            row: CSV row data
            field: Field name to look up
            
        Returns:
            Field value if found, empty string otherwise
        """
        if field not in self.field_mappings:
            return ''
            
        for possible_name in self.field_mappings[field]:
            if possible_name in row:
                return str(row[possible_name]).strip()
        
        return ''

    def _calculate_totals(self, line_items: List[OrderItem]) -> Dict[str, float]:
        """Calculate order totals from line items."""
        subtotal = 0.0
        tax_amount = 0.0
        shipping_amount = 0.0
        
        for item in line_items:
            if is_tax_product(item.productCode):
                tax_amount += item.amount
            elif is_shipping_product(item.productCode):
                shipping_amount += item.amount
            else:
                subtotal += item.amount
        
        return {
            'subtotal': subtotal,
            'tax_amount': tax_amount,
            'shipping_amount': shipping_amount
        }

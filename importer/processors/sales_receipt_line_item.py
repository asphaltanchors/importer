"""Line item processor for sales receipt data."""

from datetime import datetime
from decimal import Decimal, InvalidOperation
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

class SalesReceiptLineItemProcessor(BaseProcessor):
    """Process line items from sales receipt data."""
    
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
        
        # Field mappings specific to sales receipts
        self.field_mappings = {
            'product_code': ['Product/Service'],
            'description': ['Product/Service Description', 'Description'],  # Added 'Description' as alternative
            'quantity': ['Product/Service Quantity'],  # Match QuickBooks export format
            'rate': ['Product/Service Rate'],  # Add rate field for unit price
            'amount': ['Product/Service Amount'],  # Match QuickBooks export format
            'service_date': ['Product/Service Service Date']  # Match QuickBooks export format
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
            
            # Log column names for debugging
            self.logger.info(f"CSV columns: {df.columns.tolist()}")
            
            # Convert receipt numbers to strings to match order lookup
            df['Sales Receipt No'] = df['Sales Receipt No'].astype(str).str.strip()
            
            # Group by receipt number
            receipt_groups = df.groupby('Sales Receipt No')
            total_receipts = len(receipt_groups)
            self.logger.debug(f"Found {total_receipts} unique receipt numbers")
            
            self.logger.info(f"\nProcessing line items for {total_receipts} receipts in batches of {self.batch_size}")
            
            # Process in batches
            current_batch = []
            batch_num = 1
            
            for receipt_number, receipt_df in receipt_groups:
                if receipt_number in self.processed_orders:
                    continue
                    
                current_batch.append((receipt_number, receipt_df))
                
                if len(current_batch) >= self.batch_size:
                    self._process_batch(current_batch)
                    print(f"Batch {batch_num} complete ({len(current_batch)} receipts)", flush=True)
                    current_batch = []
                    batch_num += 1
            
            # Process final batch if any
            if current_batch:
                self._process_batch(current_batch)
                print(f"Final batch complete ({len(current_batch)} receipts)", flush=True)
            
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
            batch: List of (receipt_number, receipt_df) tuples
        """
        try:
            with self.session_manager as session:
                for receipt_number, receipt_df in batch:
                    try:
                        # Find order
                        self.logger.debug(f"Looking for order {receipt_number}")
                        order = session.query(Order).filter(
                            Order.orderNumber == str(receipt_number)
                        ).first()
                        
                        if not order:
                            self.stats['orders_not_found'] += 1
                            self.logger.error(f"Order not found for receipt {receipt_number}")
                            continue
                        
                        self.logger.debug(f"Found order {order.id} for receipt {receipt_number}")
                        
                        # Clear existing line items before processing new ones
                        self._clear_existing_line_items(order.id, session)
                        
                        # Initialize running totals
                        subtotal = Decimal('0')
                        tax_amount = Decimal('0')
                        
                        # Process each line item
                        for _, row in receipt_df.iterrows():
                            self.logger.debug(f"Processing row: {row.to_dict()}")
                            result = self._process_line_item(row, order.id, session)
                            if result['success'] and result['line_item']:
                                item = result['line_item']
                                self.stats['total_line_items'] += 1
                                
                                # Skip NaN amounts
                                if item.amount is None or pd.isna(item.amount):
                                    self.logger.debug(f"Skipping NaN amount for {item.productCode}")
                                    continue
                                
                                # Add to running totals
                                if is_tax_product(item.productCode):
                                    tax_amount += item.amount
                                    self.logger.debug(f"Added tax amount: {item.amount}")
                                else:
                                    # Everything else (including shipping) goes to subtotal
                                    subtotal += item.amount
                                    self.logger.debug(f"Added to subtotal: {item.amount}")
                        
                        # Update order totals
                        order.subtotal = subtotal
                        order.taxAmount = tax_amount
                        order.totalAmount = subtotal + tax_amount  # Shipping is included in subtotal
                        
                        self.logger.info(f"Updated order {order.orderNumber} totals:")
                        self.logger.info(f"  Subtotal: {order.subtotal}")
                        self.logger.info(f"  Tax: {order.taxAmount}")
                        self.logger.info(f"  Total: {order.totalAmount}")
                        
                        # Ensure changes are flushed to DB
                        session.flush()
                        
                        self.processed_orders.add(receipt_number)
                        self.stats['orders_processed'] += 1
                            
                    except Exception as e:
                        logging.error(f"Error processing receipt {receipt_number}: {str(e)}")
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
            
            # Parse amount first since it's always present
            amount = Decimal('0')
            amount_str = self.get_mapped_field(row, 'amount')
            if amount_str:
                try:
                    cleaned_amount = str(amount_str).replace('$', '').replace(',', '')
                    amount = Decimal(cleaned_amount)
                    self.logger.debug(f"Parsed amount: {amount}")
                except (ValueError, InvalidOperation) as e:
                    self.logger.error(f"Failed to parse amount '{amount_str}' for {product_code}: {str(e)}")
                    result['success'] = False
                    result['error'] = {
                        'severity': 'ERROR',
                        'message': f"Invalid amount for {product_code}"
                    }
                    return result

            # For special items (shipping, tax, discount), use amount as unit price and quantity of 1
            if is_tax_product(mapped_code) or is_shipping_product(mapped_code) or product_code == 'Discount':
                unit_price = amount
                quantity = Decimal('1')
                self.logger.debug(f"Special item {mapped_code}: using amount {amount} as unit price")
            else:
                # For regular products, parse rate and quantity
                rate_str = self.get_mapped_field(row, 'rate')
                if rate_str:
                    try:
                        cleaned_rate = str(rate_str).replace('$', '').replace(',', '')
                        unit_price = Decimal(cleaned_rate)
                        self.logger.debug(f"Parsed unit price from rate: {unit_price}")
                    except (ValueError, InvalidOperation) as e:
                        self.logger.error(f"Failed to parse rate '{rate_str}' for {product_code}: {str(e)}")
                        result['success'] = False
                        result['error'] = {
                            'severity': 'ERROR',
                            'message': f"Invalid rate for {product_code}"
                        }
                        return result
                else:
                    unit_price = Decimal('0')

                quantity = Decimal('1')
                quantity_str = self.get_mapped_field(row, 'quantity')
                if quantity_str:
                    try:
                        quantity = Decimal(quantity_str.strip() or '1')
                        self.logger.debug(f"Parsed quantity: {quantity}")
                    except (ValueError, InvalidOperation):
                        self.logger.warning(f"Invalid quantity for {product_code}: {quantity_str}")
                        quantity = Decimal('1')

                # Verify amount matches quantity * rate for regular products
                expected_amount = quantity * unit_price
                if amount != expected_amount:
                    self.logger.warning(
                        f"Amount mismatch for {product_code}: "
                        f"got {amount}, expected {expected_amount} "
                        f"(quantity {quantity} * rate {unit_price})"
                    )
            
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
            self.logger.debug(f"Created line item for {product_code}:")
            self.logger.debug(f"  Quantity: {quantity}")
            self.logger.debug(f"  Unit Price: {unit_price}")
            self.logger.debug(f"  Amount: {amount}")
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
            self.logger.debug(f"Field {field} not in mappings")
            return ''
            
        for possible_name in self.field_mappings[field]:
            self.logger.debug(f"Looking for {possible_name} in row")
            if possible_name in row:
                value = str(row[possible_name]).strip()
                self.logger.debug(f"Found value: {value}")
                return value
            else:
                self.logger.debug(f"{possible_name} not found in row")
        
        self.logger.debug(f"No mapping found for {field}")
        return ''

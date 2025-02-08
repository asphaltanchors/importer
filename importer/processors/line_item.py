"""Line item processor for sales data."""

from datetime import datetime
import click
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
from .error_tracker import ErrorTracker

class LineItemProcessor(BaseProcessor):
    """Process line items from sales data."""
    
    def __init__(self, session_manager, batch_size: int = 100, error_limit: int = 1000):
        """Initialize the processor.
        
        Args:
            session_manager: Database session manager
            batch_size: Number of orders to process per batch
            error_limit: Maximum number of errors before stopping
        """
        self.session_manager = session_manager
        super().__init__(None, batch_size)  # We'll manage sessions ourselves
        
        # Track processed items
        self.processed_orders: Set[str] = set()
        self.error_limit = error_limit
        self.error_tracker = ErrorTracker()
        
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
            if self.debug:
                self.logger.debug(f"Raw CSV data shape: {df.shape}")
                self.logger.debug(f"Raw columns before normalization: {df.columns.tolist()}")
            
            df = normalize_dataframe_columns(df)
            if self.debug:
                self.logger.debug(f"Normalized columns: {df.columns.tolist()}")
            
            # Validate required columns
            required_columns = ['Product/Service']
            if not validate_required_columns(df, required_columns):
                raise ValueError("Missing required columns in CSV file")
            
            # Determine if this is a sales receipt file
            is_sales_receipt = 'Sales Receipt No' in df.columns
            group_by_col = 'Sales Receipt No' if is_sales_receipt else 'Invoice No'
            
            if self.debug:
                self.logger.debug(f"CSV columns: {df.columns.tolist()}")
            
            # Convert invoice numbers to strings to match order lookup
            df[group_by_col] = df[group_by_col].astype(str).str.strip()
            
            # Group by invoice/receipt number
            invoice_groups = df.groupby(group_by_col)
            total_invoices = len(invoice_groups)
            if self.debug:
                self.logger.debug(f"Total rows in CSV: {len(df)}")
                self.logger.debug(f"Found {total_invoices} unique invoice numbers")
                # Show first few rows of data
                self.logger.debug("First few rows of normalized data:")
                for idx, row in df.head().iterrows():
                    self.logger.debug(f"Row {idx}: {row.to_dict()}")
            
            # Show validation summary
            self.logger.info("Line Item Validation Summary:")
            self.logger.info(f"Found {total_invoices} invoices with {len(df)} total line items")
            self.logger.info("Starting line item processing...")
            if self.debug:
                self.logger.debug(f"Batch size: {self.batch_size}")
            
            # Show processing plan
            self.logger.info(f"Will process in batches of {self.batch_size}")
            if self.debug:
                self.logger.debug(f"Total batches: {(len(df) + self.batch_size - 1) // self.batch_size}")
            
            # Process in batches
            current_batch = []
            batch_num = 1
            self.logger.info("Processing line items...")
            
            for invoice_number, invoice_df in invoice_groups:
                if invoice_number in self.processed_orders:
                    continue
                    
                current_batch.append((invoice_number, invoice_df))
                
                if len(current_batch) >= self.batch_size:
                    self._process_batch(current_batch)
                    if self.debug:
                        self.logger.debug(f"Batch {batch_num} complete ({len(current_batch)} invoices)")
                    current_batch = []
                    batch_num += 1
            
            # Process final batch if any
            if current_batch:
                self._process_batch(current_batch)
                if self.debug:
                    self.logger.debug(f"Final batch complete ({len(current_batch)} invoices)")
            
            return {
                'success': self.stats['failed_batches'] == 0,
                'summary': {
                    'stats': self.stats
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to process file: {str(e)}")
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
        if self.debug:
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
                        if self.debug:
                            self.logger.debug(f"Looking for order {invoice_number}")
                        order = session.query(Order).filter(
                            Order.orderNumber == str(invoice_number)
                        ).first()
                        
                        if not order:
                            self.stats['orders_not_found'] += 1
                            self.logger.error(f"Order not found for invoice {invoice_number}")
                            continue
                        
                        if self.debug:
                            self.logger.debug(f"Found order {order.id} for invoice {invoice_number}")
                        
                        # Clear existing line items before processing new ones
                        self._clear_existing_line_items(order.id, session)
                        
                        # Initialize running totals and counters
                        subtotal = Decimal('0')
                        tax_amount = Decimal('0')
                        items_found = 0
                        items_skipped = 0
                        
                        # Process each line item
                        for _, row in invoice_df.iterrows():
                            if self.debug:
                                self.logger.debug(f"Processing row: {row.to_dict()}")
                            result = self._process_line_item(row, order.id, session)
                            if result['success'] and result['line_item']:
                                item = result['line_item']
                                self.stats['total_line_items'] += 1
                                
                                # Skip NaN amounts
                                if item.amount is None or pd.isna(item.amount):
                                    if self.debug:
                                        self.logger.debug(f"Skipping NaN amount for {item.productCode}")
                                    items_skipped += 1
                                    continue
                                
                                items_found += 1
                                
                                # Add to running totals
                                if is_tax_product(item.productCode):
                                    tax_amount += item.amount
                                    if self.debug:
                                        self.logger.debug(f"Added tax amount: {item.amount}")
                                else:
                                    # Everything else (including shipping) goes to subtotal
                                    subtotal += item.amount
                                    if self.debug:
                                        self.logger.debug(f"Added to subtotal: {item.amount}")
                        
                        # Update order totals
                        order.subtotal = subtotal
                        order.taxAmount = tax_amount
                        order.totalAmount = subtotal + tax_amount  # Shipping is included in subtotal
                        
                        # Print order summary
                        if self.debug:
                            self.logger.debug(
                                f"Order {order.orderNumber}: {items_found + items_skipped} items "
                                f"(Found: {items_found}, Skipped: {items_skipped}), "
                                f"subtotal: ${subtotal:.2f}, tax: ${tax_amount:.2f}"
                            )
                            self.logger.debug(f"Updated order {order.orderNumber} totals:")
                            self.logger.debug(f"  Subtotal: {order.subtotal}")
                            self.logger.debug(f"  Tax: {order.taxAmount}")
                            self.logger.debug(f"  Total: {order.totalAmount}")
                        
                        # Ensure changes are flushed to DB
                        session.flush()
                        
                        self.processed_orders.add(invoice_number)
                        self.stats['orders_processed'] += 1
                            
                    except Exception as e:
                        self.error_tracker.add_error(
                            'INVOICE_PROCESSING_ERROR',
                            f"Error processing invoice {invoice_number}: {str(e)}",
                            {'invoice_number': invoice_number, 'error': str(e)}
                        )
                        continue
                
                # Commit batch
                session.commit()
                self.stats['successful_batches'] += 1
                
        except Exception as e:
            self.stats['failed_batches'] += 1
            self.stats['total_errors'] += 1
            self.error_tracker.add_error(
                'BATCH_PROCESSING_ERROR',
                f"Error processing batch: {str(e)}",
                {'error': str(e)}
            )
    
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
                if self.debug:
                    self.logger.debug("Skipping row - no product code found")
                return result  # Skip empty rows
            
            # Map product code using common utility
            description = self.get_mapped_field(row, 'description')
            mapped_code = map_product_code(product_code, description)
            if self.debug:
                self.logger.debug(f"Product mapping: '{product_code}' -> '{mapped_code}' (description: '{description}')")
            
            # Look up product
            product = session.query(Product).filter(
                Product.productCode == mapped_code
            ).first()
            
            if self.debug and product:
                self.logger.debug(f"Found product {product.productCode}")
            
            if not product:
                self.stats['products_not_found'] += 1
                if self.debug:
                    self.logger.debug(f"Product not found in database: {mapped_code} (original: {product_code})")
                result['success'] = False
                result['error'] = {
                    'severity': 'ERROR',
                    'message': f"Product not found: {product_code}"
                }
                return result
            
            # Parse quantity and amount
            quantity = Decimal('1')
            quantity_str = self.get_mapped_field(row, 'quantity')
            if quantity_str:
                try:
                    quantity = Decimal(quantity_str.strip() or '1')
                except (ValueError, InvalidOperation):
                    if self.debug:
                        self.logger.debug(f"Invalid quantity for {product_code}: {quantity_str}")
                    quantity = Decimal('1')

            # Debug all available fields
            if self.debug:
                self.logger.debug("Available fields in row:")
                for col in row.index:
                    self.logger.debug(f"  {col}: {row[col]}")

            amount = Decimal('0')
            amount_str = self.get_mapped_field(row, 'amount')
            if self.debug:
                self.logger.debug(f"Looking for amount using mapping: {self.field_mappings['amount']}")
                self.logger.debug(f"Raw amount for {product_code}: {amount_str}")
            
            if amount_str:
                try:
                    # Clean and parse amount
                    cleaned_amount = amount_str.replace('$', '').replace(',', '')
                    if self.debug:
                        self.logger.debug(f"Cleaned amount: {cleaned_amount}")
                    amount = Decimal(cleaned_amount)
                    if self.debug:
                        self.logger.debug(f"Parsed amount: {amount}")
                except (ValueError, InvalidOperation) as e:
                    self.logger.error(f"Failed to parse amount '{amount_str}' for {product_code}: {str(e)}")
                    result['success'] = False
                    result['error'] = {
                        'severity': 'ERROR',
                        'message': f"Invalid amount for {product_code}"
                    }
                    return result
            
            unit_price = amount / quantity if quantity != 0 else Decimal('0')
            
            # Parse service date if present
            service_date = None
            service_date_str = self.get_mapped_field(row, 'service_date')
            if service_date_str:
                try:
                    service_date = datetime.strptime(service_date_str, '%m-%d-%Y')
                except ValueError:
                    if self.debug:
                        self.logger.debug(f"Invalid service date format for {product_code}")
            
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
            if self.debug:
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
            if self.debug:
                self.logger.debug(f"Field {field} not in mappings")
            return ''
            
        for possible_name in self.field_mappings[field]:
            if self.debug:
                self.logger.debug(f"Looking for {possible_name} in row")
            if possible_name in row:
                value = str(row[possible_name]).strip()
                if self.debug:
                    self.logger.debug(f"Found value: {value}")
                return value
            elif self.debug:
                self.logger.debug(f"{possible_name} not found in row")
        
        if self.debug:
            self.logger.debug(f"No mapping found for {field}")
        return ''

    def _calculate_totals(self, line_items: List[OrderItem]) -> Dict[str, Decimal]:
        """Calculate order totals from line items."""
        subtotal = Decimal('0')
        tax_amount = Decimal('0')
        shipping_amount = Decimal('0')
        
        for item in line_items:
            # Skip items with NaN or None amounts
            if item.amount is None or pd.isna(item.amount):
                if self.debug:
                    self.logger.debug(f"Skipping line item with NaN/None amount: {item.productCode}")
                continue
                
            if is_tax_product(item.productCode):
                tax_amount += item.amount
                if self.debug:
                    self.logger.debug(f"Added tax amount: {item.amount}")
            elif is_shipping_product(item.productCode):
                shipping_amount += item.amount
                if self.debug:
                    self.logger.debug(f"Added shipping amount: {item.amount}")
            else:
                subtotal += item.amount
                if self.debug:
                    self.logger.debug(f"Added to subtotal: {item.amount}")
        
        if self.debug:
            self.logger.debug(f"Calculated totals - Subtotal: {subtotal}, Tax: {tax_amount}, Shipping: {shipping_amount}")
        
        return {
            'subtotal': subtotal,
            'tax_amount': tax_amount,
            'shipping_amount': shipping_amount
        }

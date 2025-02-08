"""Line item processor for sales receipt data."""

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, List, Set, Tuple, Union
from pathlib import Path
import logging
import pandas as pd
from sqlalchemy.orm import Session
import click

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

class SalesReceiptLineItemProcessor(BaseProcessor[Dict[str, Any]]):
    """Process line items from sales receipt data."""
    
    def __init__(
        self,
        config: Dict[str, Any],
        batch_size: int = 50,
        error_limit: int = 1000,
        debug: bool = False
    ):
        """Initialize the processor.
        
        Args:
            config: Configuration dictionary containing database_url
            batch_size: Number of orders to process per batch
            error_limit: Maximum number of errors before stopping
            debug: Enable debug logging
        """
        super().__init__(config, batch_size, error_limit, debug)
        
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
        self.stats.total_line_items = 0
        self.stats.orders_processed = 0
        self.stats.products_not_found = 0
        self.stats.orders_not_found = 0
        self.stats.skipped_items = 0
    
    def validate_data(self, df: pd.DataFrame) -> Tuple[List[str], List[str]]:
        """Validate data before processing.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (critical_issues, warnings)
        """
        critical_issues = []
        warnings = []
        
        # Check required columns (critical)
        required_columns = ['Product/Service', 'Sales Receipt No']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            critical_issues.append(f"Missing required columns: {', '.join(missing_columns)}")
            return critical_issues, warnings
        
        # Check for rows to skip (empty or missing data)
        empty_rows = df[df[['Product/Service', 'Sales Receipt No']].isna().all(axis=1)]
        if not empty_rows.empty:
            warnings.append(
                f"Found {len(empty_rows)} empty rows that will be skipped. "
                f"First few row numbers: {', '.join(map(str, empty_rows.index[:3]))}"
            )
        
        # Check for rows with missing data
        for col in ['Product/Service', 'Sales Receipt No']:
            nan_rows = df[df[col].isna() & ~df[['Product/Service', 'Sales Receipt No']].isna().all(axis=1)]
            if not nan_rows.empty:
                warnings.append(
                    f"Found {len(nan_rows)} rows with missing {col} that will be skipped. "
                    f"First few: {', '.join(map(str, nan_rows.index[:3]))}"
                )
        
        # Check for missing rates/amounts (warning)
        for col in ['Product/Service Rate', 'Product/Service Amount']:
            if col in df.columns:
                nan_count = df[col].isna().sum()
                if nan_count > 0:
                    warnings.append(
                        f"Found {nan_count} rows with missing {col} (will use defaults)"
                    )
        
        # Check service date format (warning)
        if 'Product/Service Service Date' in df.columns:
            invalid_dates = []
            for idx, date_str in df['Product/Service Service Date'].items():
                if pd.notna(date_str):
                    try:
                        datetime.strptime(str(date_str), '%m-%d-%Y')
                    except ValueError:
                        invalid_dates.append(f"Row {idx}: {date_str}")
                        if len(invalid_dates) >= 3:  # Limit samples
                            break
            if invalid_dates:
                warnings.append(
                    f"Found rows with invalid service dates (these will be skipped). "
                    f"Examples: {', '.join(invalid_dates)}"
                )
        
        return critical_issues, warnings
    
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
    
    def _process_batch(self, session: Session, batch_df: pd.DataFrame) -> pd.DataFrame:
        """Process a batch of line items.
        
        Args:
            session: Database session for this batch
            batch_df: DataFrame containing the batch data
            
        Returns:
            Processed DataFrame
        """
        # Convert receipt numbers to strings to match order lookup
        batch_df['Sales Receipt No'] = batch_df['Sales Receipt No'].astype(str).str.strip()
        
        # Group by receipt number
        receipt_groups = batch_df.groupby('Sales Receipt No')
        
        for receipt_number, receipt_df in receipt_groups:
            if receipt_number in self.processed_orders:
                continue
                
            try:
                # Find order
                self.logger.debug(f"Looking for order {receipt_number}")
                order = session.query(Order).filter(
                    Order.orderNumber == str(receipt_number)
                ).first()
                
                if not order:
                    self.stats.orders_not_found += 1
                    self.error_tracker.add_error(
                        'ORDER_NOT_FOUND',
                        f"Order not found for receipt {receipt_number}",
                        {'receipt_number': receipt_number}
                    )
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
                        self.stats.total_line_items += 1
                        
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
                
                self.logger.debug(f"Updated order {order.orderNumber} totals:")
                self.logger.debug(f"  Subtotal: {order.subtotal}")
                self.logger.debug(f"  Tax: {order.taxAmount}")
                self.logger.debug(f"  Total: {order.totalAmount}")
                
                # Ensure changes are flushed to DB
                session.flush()
                
                self.processed_orders.add(receipt_number)
                self.stats.orders_processed += 1
                    
            except Exception as e:
                self.error_tracker.add_error(
                    'RECEIPT_PROCESSING_ERROR',
                    f"Error processing receipt {receipt_number}: {str(e)}",
                    {'receipt_number': receipt_number, 'error': str(e)}
                )
                continue
        
        return batch_df
    
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
                self.stats.products_not_found += 1
                self.error_tracker.add_error(
                    'PRODUCT_NOT_FOUND',
                    f"Product not found: {product_code}",
                    {'product_code': product_code, 'description': description}
                )
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
                    error_msg = f"Failed to parse amount '{amount_str}' for {product_code}: {str(e)}"
                    self.error_tracker.add_error(
                        'INVALID_AMOUNT',
                        error_msg,
                        {'product_code': product_code, 'amount': amount_str}
                    )
                    result['success'] = False
                    result['error'] = {
                        'severity': 'ERROR',
                        'message': error_msg
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
                        error_msg = f"Failed to parse rate '{rate_str}' for {product_code}: {str(e)}"
                        self.error_tracker.add_error(
                            'INVALID_RATE',
                            error_msg,
                            {'product_code': product_code, 'rate': rate_str}
                        )
                        result['success'] = False
                        result['error'] = {
                            'severity': 'ERROR',
                            'message': error_msg
                        }
                        return result
                else:
                    unit_price = amount  # Use amount as unit price if rate not provided

                quantity = Decimal('1')
                quantity_str = self.get_mapped_field(row, 'quantity')
                if quantity_str:
                    try:
                        quantity = Decimal(quantity_str.strip() or '1')
                        self.logger.debug(f"Parsed quantity: {quantity}")
                    except (ValueError, InvalidOperation):
                        self.error_tracker.add_error(
                            'INVALID_QUANTITY',
                            f"Invalid quantity for {product_code}: {quantity_str}",
                            {'product_code': product_code, 'quantity': quantity_str}
                        )
                        quantity = Decimal('1')

                # Verify amount matches quantity * rate for regular products
                expected_amount = quantity * unit_price
                
                # Only compare amounts if we have both a valid amount and rate
                if amount and unit_price and quantity:
                    try:
                        # Convert to Decimal and round to nearest cent
                        rounded_amount = Decimal(str(amount)).quantize(Decimal('0.01'))
                        rounded_expected = (Decimal(str(unit_price)) * Decimal(str(quantity))).quantize(Decimal('0.01'))
                        
                        # Check if difference is more than 1 cent
                        if abs(rounded_amount - rounded_expected) > Decimal('0.01'):
                            self.error_tracker.add_error(
                                'AMOUNT_MISMATCH',
                                f"Amount mismatch for {product_code}: got {rounded_amount}, expected {rounded_expected}",
                                {
                                    'product_code': product_code,
                                    'actual_amount': str(rounded_amount),
                                    'expected_amount': str(rounded_expected),
                                    'quantity': str(quantity),
                                    'rate': str(unit_price)
                                }
                            )
                    except (InvalidOperation, TypeError):
                        # Skip comparison if we can't convert values to Decimal
                        pass
            
            # Parse service date if present
            service_date = None
            service_date_str = self.get_mapped_field(row, 'service_date')
            if service_date_str:
                try:
                    service_date = datetime.strptime(service_date_str, '%m-%d-%Y')
                except ValueError:
                    self.error_tracker.add_error(
                        'INVALID_SERVICE_DATE',
                        f"Invalid service date format for {product_code}: {service_date_str}",
                        {'product_code': product_code, 'service_date': service_date_str}
                    )
            
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
                amount=amount or (quantity * unit_price),  # Use calculated amount if not provided
                serviceDate=service_date,
                sourceData=source_data
            )
            
            session.add(line_item)
            self.logger.debug(f"Created line item for {product_code}:")
            self.logger.debug(f"  Quantity: {quantity}")
            self.logger.debug(f"  Unit Price: {unit_price}")
            self.logger.debug(f"  Amount: {line_item.amount}")
            result['line_item'] = line_item
            return result
            
        except Exception as e:
            error_msg = f"Failed to process line item: {str(e)}"
            self.error_tracker.add_error(
                'LINE_ITEM_PROCESSING_ERROR',
                error_msg,
                {'error': str(e)}
            )
            result['success'] = False
            result['error'] = {
                'severity': 'ERROR',
                'message': error_msg
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

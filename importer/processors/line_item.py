"""Line item processor for sales data."""

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Dict, Any, List, Set, Tuple
import logging
import pandas as pd
from sqlalchemy.orm import Session

from ..db.models import Product, OrderItem, Order
from ..utils import generate_uuid
from ..utils.product_mapping import map_product_code
from ..utils.system_products import is_tax_product, is_shipping_product
from ..utils.csv_normalization import validate_json_data
from ..db.session import SessionManager
from .base import BaseProcessor
from .error_tracker import ErrorTracker

class LineItemProcessor(BaseProcessor[Dict[str, Any]]):
    """Process line items from sales data."""
    
    def __init__(
        self,
        config: Dict[str, Any],
        batch_size: int = 100,
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
        session_manager = SessionManager(config['database_url'])
        super().__init__(session_manager, batch_size, error_limit, debug)
        
        # Track processed items
        self.processed_orders: Set[str] = set()
        self.error_tracker = ErrorTracker()
        
        # Field mappings from CSV to our schema
        self.field_mappings = {
            'product_code': ['Product/Service'],
            'description': ['Product/Service Description'],
            'quantity': ['Product/Service Quantity'],
            'amount': ['Product/Service Amount'],
            'service_date': ['Service Date']
        }
        
        # Add line item-specific stats
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
        
        # Check required columns
        required_columns = ['Product/Service']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            critical_issues.append(f"Missing required columns: {', '.join(missing_columns)}")
            return critical_issues, warnings
        
        # Check for empty product codes
        empty_products = df[df['Product/Service'].isna()]
        if not empty_products.empty:
            msg = (f"Found {len(empty_products)} rows with missing product codes that will be skipped. "
                  f"First few row numbers: {', '.join(map(str, empty_products.index[:3]))}")
            warnings.append(msg)
        
        # Check for missing amounts
        if 'Product/Service Amount' in df.columns:
            invalid_amounts = []
            for idx, amount_str in df['Product/Service Amount'].items():
                if pd.notna(amount_str):
                    try:
                        cleaned = str(amount_str).replace('$', '').replace(',', '')
                        Decimal(cleaned)
                    except InvalidOperation:
                        invalid_amounts.append(f"Row {idx}: {amount_str}")
                        if len(invalid_amounts) >= 3:
                            break
            if invalid_amounts:
                msg = (f"Found rows with invalid amounts that will be skipped. "
                      f"Examples: {', '.join(invalid_amounts)}")
                warnings.append(msg)
        
        # Check for invalid service dates
        if 'Service Date' in df.columns:
            invalid_dates = []
            for idx, date_str in df['Service Date'].items():
                if pd.notna(date_str):
                    try:
                        datetime.strptime(str(date_str), '%m-%d-%Y')
                    except ValueError:
                        invalid_dates.append(f"Row {idx}: {date_str}")
                        if len(invalid_dates) >= 3:
                            break
            if invalid_dates:
                msg = (f"Found rows with invalid service dates that will be skipped. "
                      f"Examples: {', '.join(invalid_dates)}")
                warnings.append(msg)
        
        return critical_issues, warnings
    
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

    def _process_batch(self, session: Session, batch_df: pd.DataFrame) -> pd.DataFrame:
        """Process a batch of line items.
        
        Args:
            session: Database session for this batch
            batch_df: DataFrame containing batch of rows to process
            
        Returns:
            Processed DataFrame with order IDs
        """
        if self.debug:
            self.logger.debug(f"Processing batch of {len(batch_df)} rows")
            
        # Determine if this is a sales receipt file
        is_sales_receipt = 'Sales Receipt No' in batch_df.columns
        group_by_col = 'Sales Receipt No' if is_sales_receipt else 'Invoice No'
        
        # Convert invoice numbers to strings to match order lookup
        batch_df[group_by_col] = batch_df[group_by_col].astype(str).str.strip()
        
        # Group by invoice/receipt number
        invoice_groups = batch_df.groupby(group_by_col)
        if self.debug:
            self.logger.debug(f"Found {len(invoice_groups)} unique invoice numbers")
        
        # Process each invoice
        for invoice_number, invoice_df in invoice_groups:
            try:
                if invoice_number in self.processed_orders:
                    if self.debug:
                        self.logger.debug(f"Skipping already processed invoice {invoice_number}")
                    continue
                
                # Find order
                if self.debug:
                    self.logger.debug(f"Looking for order {invoice_number}")
                order = session.query(Order).filter(
                    Order.orderNumber == str(invoice_number)
                ).first()
                
                if not order:
                    self.stats.orders_not_found += 1
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
                        self.stats.total_line_items += 1
                        
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
                self.stats.orders_processed += 1
                    
            except Exception as e:
                self.error_tracker.add_error(
                    'INVOICE_PROCESSING_ERROR',
                    f"Error processing invoice {invoice_number}: {str(e)}",
                    {'invoice_number': invoice_number, 'error': str(e)}
                )
                self.stats.total_errors += 1
                if self.debug:
                    self.logger.debug(f"Error processing invoice {invoice_number}: {str(e)}", exc_info=True)
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
                self.stats.products_not_found += 1
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

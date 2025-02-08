"""Invoice data processor."""

from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
import logging
import pandas as pd
from sqlalchemy.orm import Session

from ..utils import generate_uuid
from ..utils.normalization import normalize_customer_name, find_customer_by_name
from ..utils.product_mapping import map_product_code
from ..utils.csv_normalization import (
    normalize_dataframe_columns, 
    validate_required_columns,
    validate_json_data
)
from ..utils.system_products import is_tax_product, is_shipping_product
from ..db.models import Order, OrderStatus, PaymentStatus, Customer, Product, OrderItem
from ..db.session import SessionManager
from .address import AddressProcessor
from .base import BaseProcessor

class InvoiceProcessor(BaseProcessor[Dict[str, Any]]):
    """Process invoices from sales data."""
    
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
            batch_size: Number of records to process per batch
            error_limit: Maximum number of errors before stopping
            debug: Enable debug logging
        """
        session_manager = SessionManager(config['database_url'])
        super().__init__(session_manager, batch_size, error_limit, debug)
        self.address_processor = None  # Will be initialized per session
        
        # Add invoice-specific stats
        self.stats.total_invoices = 0
        self.stats.created = 0
        self.stats.updated = 0
        
        # Field mappings specific to invoices
        self.field_mappings = {
            'invoice_number': ['Invoice No'],
            'invoice_date': ['Invoice Date'],
            'customer_id': ['Customer'],
            'payment_terms': ['Terms'],
            'due_date': ['Due Date'],
            'po_number': ['PO Number'],
            'shipping_method': ['Ship Via'],
            'class': ['Class'],
            'payment_method': ['Payment Method']
        }

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
        required_columns = ['Invoice No', 'Invoice Date', 'Customer']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            critical_issues.append(f"Missing required columns: {', '.join(missing_columns)}")
            return critical_issues, warnings
        
        # Check for empty invoice numbers
        empty_invoices = df[df['Invoice No'].isna()]
        if not empty_invoices.empty:
            msg = (f"Found {len(empty_invoices)} rows with missing invoice numbers that will be skipped. "
                  f"First few row numbers: {', '.join(map(str, empty_invoices.index[:3]))}")
            warnings.append(msg)
        
        # Check for empty dates
        empty_dates = df[df['Invoice Date'].isna()]
        if not empty_dates.empty:
            msg = (f"Found {len(empty_dates)} rows with missing dates that will be skipped. "
                  f"First few row numbers: {', '.join(map(str, empty_dates.index[:3]))}")
            warnings.append(msg)
        
        # Check date format
        invalid_dates = []
        for idx, date_str in df['Invoice Date'].items():
            if pd.notna(date_str):
                try:
                    datetime.strptime(str(date_str), '%m-%d-%Y')
                except ValueError:
                    invalid_dates.append(f"Row {idx}: {date_str}")
                    if len(invalid_dates) >= 3:
                        break
        if invalid_dates:
            msg = (f"Found rows with invalid date formats (these will be skipped). "
                  f"Examples: {', '.join(invalid_dates)}")
            warnings.append(msg)
        
        # Check for empty customers
        empty_customers = df[df['Customer'].isna()]
        if not empty_customers.empty:
            msg = (f"Found {len(empty_customers)} rows with missing customer names that will be skipped. "
                  f"First few row numbers: {', '.join(map(str, empty_customers.index[:3]))}")
            warnings.append(msg)
        
        return critical_issues, warnings
        
    def _process_batch(self, session: Session, batch_df: pd.DataFrame) -> pd.DataFrame:
        """Process a batch of invoice rows.
        
        Args:
            session: Database session for this batch
            batch_df: DataFrame containing batch of rows to process
            
        Returns:
            Processed DataFrame with order IDs
        """
        if self.debug:
            self.logger.debug(f"Processing batch of {len(batch_df)} rows")
            
        batch_df['order_id'] = None
        
        # Map CSV headers to our standardized field names
        header_mapping = {}
        for std_field, possible_names in self.field_mappings.items():
            for name in possible_names:
                if name in batch_df.columns:
                    header_mapping[std_field] = name
                    if self.debug:
                        self.logger.debug(f"Mapped {std_field} -> {name}")
                    break
        
        if 'invoice_number' not in header_mapping:
            raise ValueError("Could not find invoice number column")
            
        # Group rows by invoice number
        if self.debug:
            self.logger.debug("Grouping rows by invoice number")
            
        invoices = {}
        for idx, row in batch_df.iterrows():
            invoice_number = str(row[header_mapping['invoice_number']]).strip()
            if invoice_number:
                if invoice_number not in invoices:
                    invoices[invoice_number] = []
                invoices[invoice_number].append((idx, row))
        
        if self.debug:
            self.logger.debug(f"Found {len(invoices)} unique invoices")
        
        # Process each invoice
        for invoice_number, invoice_rows in invoices.items():
            try:
                if self.debug:
                    self.logger.debug(f"\nProcessing invoice {invoice_number}")
                    
                idx, row = invoice_rows[0]  # Use first row for header info
                
                # Check if invoice already exists
                existing_order = session.query(Order).filter(
                    Order.orderNumber == invoice_number
                ).with_for_update().first()
                
                if self.debug and existing_order:
                    self.logger.debug(f"Found existing invoice {invoice_number}")
                
                # Get customer by name
                customer_name = row[header_mapping['customer_id']]
                if pd.isna(customer_name):
                    if self.debug:
                        self.logger.debug(f"Skipping row with missing customer name")
                    continue
                    
                customer_name = str(customer_name).strip()
                if self.debug:
                    self.logger.debug(f"Looking up customer: {customer_name}")
                
                # Use shared customer lookup logic
                customer, used_normalization = find_customer_by_name(session, customer_name)
                if customer and used_normalization:
                    if self.debug:
                        self.logger.debug(f"Found normalized name match: '{customer_name}' -> '{customer.customerName}'")
                elif customer and self.debug:
                    self.logger.debug(f"Found exact match: '{customer.customerName}'")
                elif not customer:
                    if self.debug:
                        self.logger.debug("No customer match found")
                    self.stats.total_errors += 1
                    continue
                
                # Initialize totals to 0 - will be calculated by LineItemProcessor
                subtotal = 0
                tax_amount = 0
                total_amount = 0
                now = datetime.utcnow()
                
                # Initialize address processor if needed
                if not self.address_processor:
                    if self.debug:
                        self.logger.debug("Initializing address processor")
                    self.address_processor = AddressProcessor(
                        config={'database_url': str(session.get_bind().url)},
                        batch_size=self.batch_size,
                        error_limit=self.error_limit,
                        debug=self.debug
                    )
                
                # Create addresses if provided
                if self.debug:
                    self.logger.debug("Processing addresses")
                    
                address_row = pd.Series({
                    'Billing Address Line 1': row.get('Billing Address Line 1', ''),
                    'Billing Address Line 2': row.get('Billing Address Line 2', ''),
                    'Billing Address Line 3': row.get('Billing Address Line 3', ''),
                    'Billing Address City': row.get('Billing Address City', ''),
                    'Billing Address State': row.get('Billing Address State', ''),
                    'Billing Address Postal Code': row.get('Billing Address Postal Code', ''),
                    'Billing Address Country': row.get('Billing Address Country', ''),
                    'Shipping Address Line 1': row.get('Shipping Address Line 1', ''),
                    'Shipping Address Line 2': row.get('Shipping Address Line 2', ''),
                    'Shipping Address Line 3': row.get('Shipping Address Line 3', ''),
                    'Shipping Address City': row.get('Shipping Address City', ''),
                    'Shipping Address State': row.get('Shipping Address State', ''),
                    'Shipping Address Postal Code': row.get('Shipping Address Postal Code', ''),
                    'Shipping Address Country': row.get('Shipping Address Country', '')
                })
                
                # Process addresses using DataFrame to ensure commit
                df = pd.DataFrame([address_row])
                df = self.address_processor.process(df)
                billing_id = df.at[0, 'billing_address_id']
                shipping_id = df.at[0, 'shipping_address_id']
                
                # Use customer's addresses as fallback
                billing_id = billing_id if billing_id else customer.billingAddressId
                shipping_id = shipping_id if shipping_id else customer.shippingAddressId
                
                if existing_order:
                    if self.debug:
                        self.logger.debug(f"Updating existing order: {invoice_number}")
                    
                    # Update all fields
                    existing_order.status = OrderStatus.OPEN if row.get('Status') != 'Paid' else OrderStatus.CLOSED
                    existing_order.paymentStatus = PaymentStatus.UNPAID if row.get('Status') != 'Paid' else PaymentStatus.PAID
                    existing_order.subtotal = subtotal
                    existing_order.taxAmount = tax_amount
                    existing_order.totalAmount = total_amount
                    existing_order.customerId = customer.id
                    existing_order.billingAddressId = billing_id
                    existing_order.shippingAddressId = shipping_id
                    existing_order.terms = row.get(header_mapping.get('payment_terms', ''), '')
                    existing_order.dueDate = datetime.strptime(row[header_mapping['due_date']], '%m-%d-%Y') if header_mapping.get('due_date') and row.get(header_mapping['due_date']) else None
                    existing_order.poNumber = row.get(header_mapping.get('po_number', ''), '')
                    existing_order.class_ = row.get(header_mapping.get('class', ''), '')
                    existing_order.shippingMethod = row.get(header_mapping.get('shipping_method', ''), '')
                    existing_order.paymentMethod = row.get(header_mapping.get('payment_method', ''), 'Invoice')  # Default to Invoice
                    existing_order.quickbooksId = row.get('QuickBooks Internal Id', '')
                    existing_order.modifiedAt = now
                    existing_order.sourceData = validate_json_data(row.to_dict())
                    
                    order = existing_order
                    self.stats.updated += 1
                else:
                    if self.debug:
                        self.logger.debug(f"Creating new order: {invoice_number}")
                    order = Order(
                        id=generate_uuid(),
                        orderNumber=invoice_number,
                        customerId=customer.id,
                        orderDate=datetime.strptime(row[header_mapping['invoice_date']], '%m-%d-%Y'),
                        status=OrderStatus.OPEN if row.get('Status') != 'Paid' else OrderStatus.CLOSED,
                        paymentStatus=PaymentStatus.UNPAID if row.get('Status') != 'Paid' else PaymentStatus.PAID,
                        subtotal=subtotal,
                        taxPercent=None,
                        taxAmount=tax_amount,
                        totalAmount=total_amount,
                        billingAddressId=billing_id,
                        shippingAddressId=shipping_id,
                        terms=row.get(header_mapping.get('payment_terms', ''), ''),
                        dueDate=datetime.strptime(row[header_mapping['due_date']], '%m-%d-%Y') if header_mapping.get('due_date') and row.get(header_mapping['due_date']) else None,
                        poNumber=row.get(header_mapping.get('po_number', ''), ''),
                        class_=row.get(header_mapping.get('class', ''), ''),
                        shippingMethod=row.get(header_mapping.get('shipping_method', ''), ''),
                        paymentMethod=row.get(header_mapping.get('payment_method', ''), 'Invoice'),  # Default to Invoice
                        quickbooksId=row.get('QuickBooks Internal Id', ''),
                        createdAt=now,
                        modifiedAt=now,
                        sourceData=validate_json_data(row.to_dict())
                    )
                    session.add(order)
                    self.stats.created += 1
                
                # Update order ID in DataFrame
                for idx, _ in invoice_rows:
                    batch_df.at[idx, 'order_id'] = order.id
                
                self.stats.total_invoices += 1
                
                if self.debug:
                    self.logger.debug(f"Successfully processed invoice {invoice_number}")
                
            except Exception as e:
                self.logger.error(f"Failed to process invoice {invoice_number}: {str(e)}")
                if self.debug:
                    self.logger.debug(f"Error details:", exc_info=True)
                self.stats.total_errors += 1
                continue
        
        return batch_df

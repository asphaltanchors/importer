"""Sales receipt data processor."""

from typing import Dict, Any, List, Optional, Tuple
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from sqlalchemy import func
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
from .address import AddressProcessor
from .base import BaseProcessor

class SalesReceiptProcessor(BaseProcessor):
    """Process sales receipts from sales data."""
    
    def __init__(self, session: Session, batch_size: int = 100):
        """Initialize the processor.
        
        Args:
            session: Database session
            batch_size: Number of records to process per batch
        """
        super().__init__(session, batch_size)
        self.address_processor = None  # Will be initialized per session
        self.stats.update({
            'total_receipts': 0,
            'created': 0,
            'updated': 0,
            'errors': 0
        })
        
        # Field mappings specific to sales receipts
        self.field_mappings = {
            'receipt_number': ['Sales Receipt No'],
            'receipt_date': ['Sales Receipt Date'],
            'customer_id': ['Customer'],
            'payment_method': ['Payment Method'],
            'class': ['Class'],
            'shipping_method': ['Ship Via']
        }
        
    def _process_batch(self, batch_df: pd.DataFrame) -> pd.DataFrame:
        """Process a batch of sales receipt rows.
        
        Args:
            batch_df: DataFrame containing batch of rows to process
            
        Returns:
            Processed DataFrame with order IDs
        """
        batch_df['order_id'] = None
        
        # Map CSV headers to our standardized field names
        header_mapping = {}
        for std_field, possible_names in self.field_mappings.items():
            for name in possible_names:
                if name in batch_df.columns:
                    header_mapping[std_field] = name
                    self.logger.debug(f"Mapped {std_field} -> {name}")
                    break
        
        if 'receipt_number' not in header_mapping:
            raise ValueError("Could not find sales receipt number column")
            
        # Group rows by receipt number
        receipts = {}
        for idx, row in batch_df.iterrows():
            receipt_number = str(row[header_mapping['receipt_number']]).strip()
            if receipt_number:
                if receipt_number not in receipts:
                    receipts[receipt_number] = []
                receipts[receipt_number].append((idx, row))
        
        # Process each receipt
        for receipt_number, receipt_rows in receipts.items():
            try:
                idx, row = receipt_rows[0]  # Use first row for header info
                
                # Check if receipt already exists
                existing_order = self.session.query(Order).filter(
                    Order.orderNumber == receipt_number
                ).with_for_update().first()
                
                # Get customer by name
                customer_name = row[header_mapping['customer_id']]
                if pd.isna(customer_name):
                    self.logger.warning(f"Skipping row with missing customer name")
                    continue
                    
                customer_name = str(customer_name).strip()
                self.logger.debug(f"Looking up customer: {customer_name}")
                
                # Use shared customer lookup logic
                customer, used_normalization = find_customer_by_name(self.session, customer_name)
                if customer and used_normalization:
                    self.logger.info(f"Found normalized name match: '{customer_name}' -> '{customer.customerName}'")
                elif customer:
                    self.logger.debug(f"Found exact match: '{customer.customerName}'")
                else:
                    self.logger.debug("No customer match found")
                    self.stats['errors'] += 1
                    continue
                
                # Initialize totals to 0 - will be calculated by LineItemProcessor
                subtotal = 0
                tax_amount = 0
                total_amount = 0
                now = datetime.utcnow()
                
                # Initialize address processor if needed
                if not self.address_processor:
                    self.address_processor = AddressProcessor(self.session)
                
                # Create addresses if provided
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
                    # Update existing order
                    self.logger.info(f"Updating existing order: {receipt_number}")
                    
                    # Update all fields
                    existing_order.status = OrderStatus.CLOSED  # Sales receipts are always closed
                    existing_order.paymentStatus = PaymentStatus.PAID  # Sales receipts are always paid
                    existing_order.subtotal = subtotal
                    existing_order.taxAmount = tax_amount
                    existing_order.totalAmount = total_amount
                    existing_order.customerId = customer.id
                    existing_order.billingAddressId = billing_id
                    existing_order.shippingAddressId = shipping_id
                    existing_order.class_ = row.get(header_mapping.get('class', ''), '')
                    existing_order.shippingMethod = row.get(header_mapping.get('shipping_method', ''), '')
                    existing_order.paymentMethod = row.get(header_mapping.get('payment_method', ''), 'Cash')  # Default to Cash for sales receipts
                    existing_order.quickbooksId = row.get('QuickBooks Internal Id', '')
                    existing_order.modifiedAt = now
                    existing_order.sourceData = validate_json_data(row.to_dict())
                    
                    order = existing_order
                    self.stats['updated'] += 1
                else:
                    # Create new order
                    self.logger.info(f"Creating new order: {receipt_number}")
                    order = Order(
                        id=generate_uuid(),
                        orderNumber=receipt_number,
                        customerId=customer.id,
                        orderDate=datetime.strptime(row[header_mapping['receipt_date']], '%m-%d-%Y'),
                        status=OrderStatus.CLOSED,  # Sales receipts are always closed
                        paymentStatus=PaymentStatus.PAID,  # Sales receipts are always paid
                        subtotal=subtotal,
                        taxPercent=None,
                        taxAmount=tax_amount,
                        totalAmount=total_amount,
                        billingAddressId=billing_id,
                        shippingAddressId=shipping_id,
                        class_=row.get(header_mapping.get('class', ''), ''),
                        shippingMethod=row.get(header_mapping.get('shipping_method', ''), ''),
                        paymentMethod=row.get(header_mapping.get('payment_method', ''), 'Cash'),  # Default to Cash for sales receipts
                        quickbooksId=row.get('QuickBooks Internal Id', ''),
                        createdAt=now,
                        modifiedAt=now,
                        sourceData=validate_json_data(row.to_dict())
                    )
                    self.session.add(order)
                    self.stats['created'] += 1
                
                # Update order ID in DataFrame
                for idx, _ in receipt_rows:
                    batch_df.at[idx, 'order_id'] = order.id
                
                self.stats['total_receipts'] += 1
                
            except Exception as e:
                self.logger.error(f"Failed to process sales receipt {receipt_number}: {str(e)}")
                self.stats['errors'] += 1
                continue
        
        return batch_df

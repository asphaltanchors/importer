"""Order processor for sales data."""

from datetime import datetime
from typing import Dict, Any, Optional
import logging
from sqlalchemy.orm import Session

from ..db.models import Order, OrderStatus, PaymentStatus, Customer
from ..utils import generate_uuid
from ..utils.normalization import normalize_customer_name
from .address import AddressProcessor

class OrderProcessor:
    """Process orders from sales data."""
    
    def __init__(self, session: Session):
        """Initialize the processor.
        
        Args:
            session: SQLAlchemy session for database operations
        """
        self.session = session
        self.address_processor = AddressProcessor(session)
        
        # Field mappings from CSV to our schema
        self.field_mappings = {
            'invoice_number': ['Invoice No', 'Sales Receipt No'],
            'invoice_date': ['Invoice Date', 'Sales Receipt Date'],
            'customer_id': ['Customer'],  # QuickBooks ID we map to our customer
            'payment_terms': ['Terms'],
            'due_date': ['Due Date'],
            'po_number': ['PO Number'],
            'shipping_method': ['Ship Via'],
            'class': ['Class'],
            'payment_method': ['Payment Method']
        }
    
    def get_mapped_field(self, row: Dict[str, str], field: str) -> Optional[str]:
        """Get value for a mapped field from the row.
        
        Args:
            row: CSV row data
            field: Field name to look up
            
        Returns:
            Field value if found, None otherwise
        """
        if field not in self.field_mappings:
            return None
            
        for possible_name in self.field_mappings[field]:
            if possible_name in row:
                return row[possible_name].strip()
        
        return None
    
    def find_customer(self, customer_name: str) -> Optional[Customer]:
        """Find customer by name with normalization fallback.
        
        Args:
            customer_name: Customer name to look up
            
        Returns:
            Customer if found, None otherwise
        """
        # Try exact match first
        customer = self.session.query(Customer).filter(
            Customer.customerName == customer_name
        ).first()
        
        # If not found, try normalized match
        if not customer:
            normalized_name = normalize_customer_name(customer_name)
            for existing in self.session.query(Customer).all():
                if normalize_customer_name(existing.customerName) == normalized_name:
                    customer = existing
                    logging.info(f"Found normalized name match: '{customer_name}' -> '{existing.customerName}'")
                    break
        
        return customer
    
    def process_order(self, row: Dict[str, str], is_sales_receipt: bool = False) -> Dict[str, Any]:
        """Process a single order from a CSV row.
        
        Args:
            row: CSV row containing order data
            is_sales_receipt: Whether this is a sales receipt vs invoice
            
        Returns:
            Dict containing processing results with structure:
            {
                'success': bool,
                'order': Optional[Order],
                'error': Optional[Dict]
            }
        """
        result = {
            'success': True,
            'order': None,
            'error': None
        }
        
        try:
            # Get invoice number
            invoice_number = self.get_mapped_field(row, 'invoice_number')
            if not invoice_number:
                result['success'] = False
                result['error'] = {
                    'severity': 'ERROR',
                    'message': 'Missing invoice number'
                }
                return result
            
            # Check if order already exists
            existing_order = self.session.query(Order).filter(
                Order.orderNumber == invoice_number
            ).first()
            
            # Get customer
            customer_name = self.get_mapped_field(row, 'customer_id')
            if not customer_name:
                result['success'] = False
                result['error'] = {
                    'severity': 'ERROR',
                    'message': f"Missing customer name for invoice {invoice_number}"
                }
                return result
            
            customer = self.find_customer(customer_name)
            if not customer:
                result['success'] = False
                result['error'] = {
                    'severity': 'ERROR',
                    'message': f"Customer not found: {customer_name}"
                }
                return result
            
            # Process addresses
            billing_id = customer.billingAddressId
            shipping_id = customer.shippingAddressId
            
            # Create addresses if provided in row
            if any(row.get(f"Billing Address {field}", '').strip() for field in ['Line 1', 'City', 'State']):
                import pandas as pd
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
                billing_id = df.at[0, 'billing_address_id'] or billing_id
                shipping_id = df.at[0, 'shipping_address_id'] or shipping_id
            
            now = datetime.utcnow()
            
            # Get order date
            order_date = None
            order_date_str = self.get_mapped_field(row, 'invoice_date')
            if order_date_str:
                try:
                    order_date = datetime.strptime(order_date_str, '%m-%d-%Y')
                except ValueError:
                    result['success'] = False
                    result['error'] = {
                        'severity': 'ERROR',
                        'message': f"Invalid order date format: {order_date_str}"
                    }
                    return result
            
            # Get due date if present
            due_date = None
            due_date_str = self.get_mapped_field(row, 'due_date')
            if due_date_str:
                try:
                    due_date = datetime.strptime(due_date_str, '%m-%d-%Y')
                except ValueError:
                    logging.warning(f"Invalid due date format: {due_date_str}")
            
            # Determine order status
            is_paid = is_sales_receipt or row.get('Status') == 'Paid'
            status = OrderStatus.CLOSED if is_paid else OrderStatus.OPEN
            payment_status = PaymentStatus.PAID if is_paid else PaymentStatus.UNPAID
            
            if existing_order:
                # Update existing order
                existing_order.status = status
                existing_order.paymentStatus = payment_status
                existing_order.customerId = customer.id
                existing_order.billingAddressId = billing_id
                existing_order.shippingAddressId = shipping_id
                existing_order.terms = self.get_mapped_field(row, 'payment_terms') or ''
                existing_order.dueDate = due_date
                existing_order.poNumber = self.get_mapped_field(row, 'po_number') or ''
                existing_order.class_ = self.get_mapped_field(row, 'class') or ''
                existing_order.shippingMethod = self.get_mapped_field(row, 'shipping_method') or ''
                existing_order.paymentMethod = self.get_mapped_field(row, 'payment_method') or 'Invoice'
                existing_order.quickbooksId = row.get('QuickBooks Internal Id', '')
                existing_order.modifiedAt = now
                existing_order.sourceData = row
                
                result['order'] = existing_order
                
            else:
                # Create new order
                order = Order(
                    id=generate_uuid(),
                    orderNumber=invoice_number,
                    customerId=customer.id,
                    orderDate=order_date,
                    status=status,
                    paymentStatus=payment_status,
                    subtotal=0,  # Will be updated when line items are linked
                    taxPercent=None,
                    taxAmount=0,  # Will be updated when line items are linked
                    totalAmount=0,  # Will be updated when line items are linked
                    billingAddressId=billing_id,
                    shippingAddressId=shipping_id,
                    terms=self.get_mapped_field(row, 'payment_terms') or '',
                    dueDate=due_date,
                    poNumber=self.get_mapped_field(row, 'po_number') or '',
                    class_=self.get_mapped_field(row, 'class') or '',
                    shippingMethod=self.get_mapped_field(row, 'shipping_method') or '',
                    paymentMethod=self.get_mapped_field(row, 'payment_method') or 'Invoice',
                    quickbooksId=row.get('QuickBooks Internal Id', ''),
                    createdAt=now,
                    modifiedAt=now,
                    sourceData=row
                )
                
                result['order'] = order
            
            return result
            
        except Exception as e:
            result['success'] = False
            result['error'] = {
                'severity': 'ERROR',
                'message': f"Failed to process order: {str(e)}"
            }
            return result

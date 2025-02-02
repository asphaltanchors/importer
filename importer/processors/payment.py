"""Payment processor for sales data."""

from datetime import datetime
from typing import Dict, Any, Optional
import logging
from sqlalchemy.orm import Session

from ..db.models import Order, OrderStatus, PaymentStatus
from ..utils import generate_uuid

class PaymentProcessor:
    """Process payment information from sales data."""
    
    def __init__(self, session: Session):
        """Initialize the processor.
        
        Args:
            session: SQLAlchemy session for database operations
        """
        self.session = session
        
        # Field mappings from CSV to our schema
        self.field_mappings = {
            'invoice_number': ['Invoice No', 'Sales Receipt No'],
            'payment_terms': ['Terms'],
            'payment_method': ['Payment Method'],
            'payment_status': ['Status'],
            'payment_amount': ['Total Amount'],
            'due_date': ['Due Date']
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
    
    def process_payment(self, row: Dict[str, str], is_sales_receipt: bool = False) -> Dict[str, Any]:
        """Process payment information from a CSV row.
        
        Args:
            row: CSV row containing payment data
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
            
            # Find order
            order = self.session.query(Order).filter(
                Order.orderNumber == invoice_number
            ).first()
            
            if not order:
                result['success'] = False
                result['error'] = {
                    'severity': 'ERROR',
                    'message': f"Order not found: {invoice_number}"
                }
                return result
            
            # Get payment amount
            payment_amount = 0.0
            amount_str = self.get_mapped_field(row, 'payment_amount')
            if amount_str:
                try:
                    payment_amount = float(amount_str.replace('$', '').replace(',', ''))
                except ValueError:
                    result['success'] = False
                    result['error'] = {
                        'severity': 'ERROR',
                        'message': f"Invalid payment amount: {amount_str}"
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
            
            # Determine payment status
            is_paid = is_sales_receipt or self.get_mapped_field(row, 'payment_status') == 'Paid'
            status = OrderStatus.CLOSED if is_paid else OrderStatus.OPEN
            payment_status = PaymentStatus.PAID if is_paid else PaymentStatus.UNPAID
            
            # Update order payment info
            order.status = status
            order.paymentStatus = payment_status
            order.totalAmount = payment_amount
            order.terms = self.get_mapped_field(row, 'payment_terms') or ''
            order.dueDate = due_date
            order.paymentMethod = self.get_mapped_field(row, 'payment_method') or 'Invoice'
            order.modifiedAt = datetime.utcnow()
            
            result['order'] = order
            return result
            
        except Exception as e:
            result['success'] = False
            result['error'] = {
                'severity': 'ERROR',
                'message': f"Failed to process payment: {str(e)}"
            }
            return result

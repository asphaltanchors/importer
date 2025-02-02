"""Line item processor for sales data."""

from datetime import datetime
from typing import Dict, Any, List
import logging
from sqlalchemy.orm import Session

from ..db.models import Product, OrderItem
from ..utils import generate_uuid

class LineItemProcessor:
    """Process line items from sales data."""
    
    def __init__(self, session: Session):
        """Initialize the processor.
        
        Args:
            session: SQLAlchemy session for database operations
        """
        self.session = session
        
    def process_row(self, row: Dict[str, str], order_id: str) -> Dict[str, Any]:
        """Process a single line item row.
        
        Args:
            row: Dictionary containing line item data
            order_id: ID of the parent order
            
        Returns:
            Dict containing processing results with structure:
            {
                'success': bool,
                'line_item': Optional[OrderItem],
                'error': Optional[Dict]
            }
        """
        result = {
            'success': True,
            'line_item': None,
            'error': None
        }
        
        try:
            product_code = row.get('Product/Service', '').strip()
            if not product_code:
                return result  # Skip empty rows
            
            # Map special items to system product codes
            product_code_lower = product_code.lower()
            if product_code_lower == 'shipping':
                mapped_code = 'SYS-SHIPPING'
            elif product_code_lower == 'handling fee':
                mapped_code = 'SYS-HANDLING'
            elif product_code_lower == 'tax':
                mapped_code = 'SYS-TAX'
            elif product_code_lower == 'nj sales tax':
                mapped_code = 'SYS-NJ-TAX'
            elif product_code_lower == 'discount':
                mapped_code = 'SYS-DISCOUNT'
            else:
                mapped_code = product_code.upper()
            
            # Look up product
            product = self.session.query(Product).filter(
                Product.productCode == mapped_code
            ).first()
            
            if not product:
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
                sourceData=row
            )
            
            result['line_item'] = line_item
            return result
            
        except Exception as e:
            result['success'] = False
            result['error'] = {
                'severity': 'ERROR',
                'message': f"Failed to process line item: {str(e)}"
            }
            return result
    
    def calculate_totals(self, line_items: List[OrderItem]) -> Dict[str, float]:
        """Calculate order totals from line items.
        
        Args:
            line_items: List of processed line items
            
        Returns:
            Dict containing:
            {
                'subtotal': float,  # Sum of non-tax items
                'tax_amount': float  # Sum of tax items
            }
        """
        subtotal = 0.0
        tax_amount = 0.0
        
        for item in line_items:
            if item.productCode in ['SYS-TAX', 'SYS-NJ-TAX']:
                tax_amount += item.amount
            else:
                subtotal += item.amount
        
        return {
            'subtotal': subtotal,
            'tax_amount': tax_amount
        }

"""Sales data verification processor."""

from pathlib import Path
import logging
from typing import List, Dict, Any
from sqlalchemy.orm import Session
from decimal import Decimal

from .base import BaseProcessor
from ..cli.logging import get_logger
from ..db.session import SessionManager
from ..db.models import (
    Order, OrderItem, Product, Customer,
    OrderStatus, PaymentStatus
)

class SalesVerifier(BaseProcessor):
    """Verifies the integrity of imported sales data."""

    def __init__(self, config):
        super().__init__(config)
        self.issues: List[Dict[str, Any]] = []
        self.logger = get_logger(__name__)
        self.stats = {
            'orders': 0,
            'line_items': 0,
            'products': 0,
            'customers': 0,
            'issues': 0
        }

    def verify(self, file: Path) -> None:
        """Verify data integrity for the given file.
        
        Args:
            file: Path to the file being verified
        """
        with SessionManager(self.config['database_url']) as session:
            # Basic reference checks
            self.verify_customer_references(session)
            self.verify_product_references(session)
            
            # Line item verification
            self.verify_line_items(session)
            
            # Order verification
            self.verify_order_totals(session)
            self.verify_order_status(session)
            
            # Payment verification
            self.verify_payment_status(session)
            
            # Orphan detection
            self.verify_no_orphans(session)
            
            # Print summary
            self.print_summary()

    def verify_customer_references(self, session: Session) -> None:
        """Verify all orders link to valid customers."""
        orders_without_customers = session.query(Order).filter(
            ~Order.customerId.in_(
                session.query(Customer.id)
            )
        ).all()
        
        self.stats['orders'] = session.query(Order).count()
        self.stats['customers'] = session.query(Customer).count()
        
        if orders_without_customers:
            for order in orders_without_customers:
                self.add_issue("Invalid Customer Reference", 
                    f"Order {order.orderNumber} references non-existent customer {order.customerId}")

    def verify_product_references(self, session: Session) -> None:
        """Verify all order items link to valid products."""
        items_without_products = session.query(OrderItem).filter(
            ~OrderItem.productCode.in_(
                session.query(Product.productCode)
            )
        ).all()
        
        self.stats['products'] = session.query(Product).count()
        
        if items_without_products:
            for item in items_without_products:
                self.add_issue("Invalid Product Reference",
                    f"Order item {item.id} references non-existent product {item.productCode}")

    def verify_line_items(self, session: Session) -> None:
        """Verify line item integrity."""
        # Count line items
        self.stats['line_items'] = session.query(OrderItem).count()
        
        # Check for items with invalid quantities or prices
        # Allow negative amounts for discounts and zero amounts for shipping/handling
        invalid_items = session.query(OrderItem).filter(
            (
                (OrderItem.quantity <= 0) |
                (OrderItem.unitPrice < 0) |
                (OrderItem.amount < 0)
            ) &
            ~OrderItem.productCode.in_(['SYS-DISCOUNT', 'SYS-SHIPPING', 'SYS-HANDLING'])  # Exclude special items from validation
        ).all()
        
        if invalid_items:
            for item in invalid_items:
                self.add_issue("Invalid Line Item",
                    f"Order item {item.id} has invalid quantity ({item.quantity}) or price ({item.unitPrice})")
        
        # Check amount calculations
        items = session.query(OrderItem).all()
        for item in items:
            expected_amount = Decimal(str(item.quantity)) * Decimal(str(item.unitPrice))
            if abs(Decimal(str(item.amount)) - expected_amount) > Decimal('0.01'):
                self.add_issue("Line Item Amount Mismatch",
                    f"Order item {item.id} amount ({item.amount}) does not match quantity * price ({expected_amount})")

    def verify_order_totals(self, session: Session) -> None:
        """Verify order totals match sum of line items."""
        orders = session.query(Order).all()
        
        for order in orders:
            items = session.query(OrderItem).filter(OrderItem.orderId == order.id).all()
            
            # Define product code categories
            tax_codes = ['SYS-TAX', 'SYS-NJ-TAX']
            shipping_codes = ['SYS-SHIPPING']  # All shipping charges use SYS-SHIPPING
            
            # Calculate amounts by category
            tax_amount = sum(
                item.amount for item in items 
                if item.productCode in tax_codes
            )
            
            # Include shipping even if amount is 0 (collect shipments)
            shipping_items = [item for item in items if item.productCode in shipping_codes]
            shipping_amount = sum(item.amount for item in shipping_items)
            has_shipping = len(shipping_items) > 0
            
            # Calculate subtotal (excluding tax and shipping)
            subtotal = sum(
                item.amount for item in items 
                if item.productCode not in tax_codes + shipping_codes
            )
            
            # For orders with shipping items but zero shipping amount (collect shipments),
            # use the total amount minus subtotal and tax
            if has_shipping and shipping_amount == 0:
                order_total = Decimal(str(order.totalAmount))
                shipping_amount = order_total - subtotal - tax_amount
            
            # Define minimum difference threshold for reporting mismatches
            min_diff = Decimal('0.01')
            
            # Convert values to Decimal for accurate comparison
            order_subtotal = Decimal(str(order.subtotal))
            order_tax = Decimal(str(order.taxAmount))
            order_total = Decimal(str(order.totalAmount))
            calc_subtotal = Decimal(str(subtotal))
            calc_tax = Decimal(str(tax_amount))
            calc_shipping = Decimal(str(shipping_amount))
            
            # Verify subtotal
            if abs(calc_subtotal - order_subtotal) > min_diff:
                # Only report if difference is significant
                self.add_issue("Order Subtotal Mismatch",
                    f"Order {order.orderNumber} subtotal ({order_subtotal:.2f}) does not match sum of line items ({calc_subtotal:.2f})")
            
            # Verify tax amount
            if abs(calc_tax - order_tax) > min_diff:
                # Only report if difference is significant
                self.add_issue("Order Tax Mismatch",
                    f"Order {order.orderNumber} tax amount ({order_tax:.2f}) does not match sum of tax items ({calc_tax:.2f})")
            
            # Verify total amount (including shipping and handling)
            calc_total = calc_subtotal + calc_tax + calc_shipping
            if abs(calc_total - order_total) > min_diff:
                # Only report if difference is significant
                self.add_issue("Order Total Mismatch",
                    f"Order {order.orderNumber} total ({order_total:.2f}) does not match subtotal + tax + shipping/handling ({calc_total:.2f})")

    def verify_order_status(self, session: Session) -> None:
        """Verify order status consistency."""
        orders = session.query(Order).all()
        
        for order in orders:
            # Sales receipts should always be CLOSED/PAID
            if order.orderNumber.startswith('SR'):
                if order.status != OrderStatus.CLOSED:
                    self.add_issue("Invalid Order Status",
                        f"Sales receipt {order.orderNumber} has invalid status {order.status}")
                if order.paymentStatus != PaymentStatus.PAID:
                    self.add_issue("Invalid Payment Status",
                        f"Sales receipt {order.orderNumber} has invalid payment status {order.paymentStatus}")

    def verify_payment_status(self, session: Session) -> None:
        """Verify payment status consistency."""
        orders = session.query(Order).all()
        
        for order in orders:
            # CLOSED orders should be PAID
            if order.status == OrderStatus.CLOSED and order.paymentStatus != PaymentStatus.PAID:
                self.add_issue("Status Mismatch",
                    f"Order {order.orderNumber} is CLOSED but payment status is {order.paymentStatus}")
            
            # PAID orders should be CLOSED
            if order.paymentStatus == PaymentStatus.PAID and order.status != OrderStatus.CLOSED:
                self.add_issue("Status Mismatch",
                    f"Order {order.orderNumber} is PAID but order status is {order.status}")

    def verify_no_orphans(self, session: Session) -> None:
        """Verify no orphaned records exist."""
        # Check for order items without orders
        orphaned_items = session.query(OrderItem).filter(
            ~OrderItem.orderId.in_(
                session.query(Order.id)
            )
        ).all()
        
        if orphaned_items:
            for item in orphaned_items:
                self.add_issue("Orphaned Record",
                    f"Order item {item.id} not linked to any order")

    def add_issue(self, issue_type: str, message: str) -> None:
        """Add an issue to the list."""
        self.issues.append({
            "type": issue_type,
            "message": message
        })
        self.stats['issues'] += 1

    def print_summary(self) -> None:
        """Print verification summary."""
        self.logger.info("Verification Summary:")
        self.logger.info(f"  Orders: {self.stats['orders']}")
        self.logger.info(f"  Line Items: {self.stats['line_items']}")
        self.logger.info(f"  Products: {self.stats['products']}")
        self.logger.info(f"  Customers: {self.stats['customers']}")
        
        if self.issues:
            self.logger.warning(f"\nFound {len(self.issues)} issues:")
            for issue in self.issues:
                self.logger.warning(f"  {issue['type']}: {issue['message']}")
        else:
            self.logger.info("\nNo issues found - verification passed")

"""
Verification processors for imported data.
"""

from pathlib import Path
import logging
from typing import List, Dict, Any
from sqlalchemy.orm import Session

from .base import BaseProcessor
from ..cli.logging import get_logger
from ..db.session import SessionManager
from ..db.models.order import Order
from ..db.models.order_item import OrderItem
from ..db.models.product import Product
from ..db.models.customer import Customer
from ..db.models.company import Company
from ..db.models.address import Address
from ..db.models.customer_email import CustomerEmail
from ..db.models.customer_phone import CustomerPhone


class ImportVerifier:
    """Verifies the integrity of imported customer data."""

    def __init__(self, session: Session):
        self.session = session

    def verify_import(self) -> Dict[str, Any]:
        """
        Verify the integrity of imported customer data.
        
        Returns:
            Dict containing verification results and statistics
        """
        results = {
            'success': True,
            'summary': {
                'customers': self._get_customer_stats(),
                'orphaned': self._get_orphaned_stats(),
                'relationships': self._get_relationship_stats()
            },
            'relationship_issues': self._get_relationship_issues(),
            'orphaned_records': self._get_orphaned_records()
        }
        
        # Set success flag based on issues found
        if (results['relationship_issues'] or 
            results['orphaned_records'] or 
            results['summary']['relationships']['invalid_company_refs'] > 0 or
            results['summary']['relationships']['invalid_address_refs'] > 0):
            results['success'] = False
            
        return results

    def _get_customer_stats(self) -> Dict[str, int]:
        """Get customer-related statistics."""
        total = self.session.query(Customer).count()
        with_company = self.session.query(Customer).filter(Customer.company_id.isnot(None)).count()
        with_billing = self.session.query(Customer).filter(Customer.billing_address_id.isnot(None)).count()
        with_shipping = self.session.query(Customer).filter(Customer.shipping_address_id.isnot(None)).count()
        with_emails = self.session.query(Customer).join(CustomerEmail).group_by(Customer.id).count()
        with_phones = self.session.query(Customer).join(CustomerPhone).group_by(Customer.id).count()
        
        return {
            'total': total,
            'with_company': with_company,
            'with_billing_address': with_billing,
            'with_shipping_address': with_shipping,
            'with_emails': with_emails,
            'with_phones': with_phones
        }

    def _get_orphaned_stats(self) -> Dict[str, int]:
        """Get statistics about orphaned records."""
        orphaned_addresses = self.session.query(Address).filter(
            ~Address.id.in_(
                self.session.query(Customer.billing_address_id).union(
                    self.session.query(Customer.shipping_address_id)
                )
            )
        ).count()
        
        orphaned_emails = self.session.query(CustomerEmail).filter(
            ~CustomerEmail.customer_id.in_(
                self.session.query(Customer.id)
            )
        ).count()
        
        orphaned_phones = self.session.query(CustomerPhone).filter(
            ~CustomerPhone.customer_id.in_(
                self.session.query(Customer.id)
            )
        ).count()
        
        return {
            'addresses': orphaned_addresses,
            'emails': orphaned_emails,
            'phones': orphaned_phones
        }

    def _get_relationship_stats(self) -> Dict[str, int]:
        """Get statistics about relationship issues."""
        invalid_company = self.session.query(Customer).filter(
            Customer.company_id.isnot(None),
            ~Customer.company_id.in_(
                self.session.query(Company.id)
            )
        ).count()
        
        invalid_address = self.session.query(Customer).filter(
            (Customer.billing_address_id.isnot(None) & 
             ~Customer.billing_address_id.in_(self.session.query(Address.id))) |
            (Customer.shipping_address_id.isnot(None) & 
             ~Customer.shipping_address_id.in_(self.session.query(Address.id)))
        ).count()
        
        return {
            'invalid_company_refs': invalid_company,
            'invalid_address_refs': invalid_address
        }

    def _get_relationship_issues(self) -> List[Dict[str, Any]]:
        """Get detailed information about relationship issues."""
        issues = []
        
        # Check company references
        invalid_companies = self.session.query(Customer).filter(
            Customer.company_id.isnot(None),
            ~Customer.company_id.in_(
                self.session.query(Company.id)
            )
        ).all()
        
        for customer in invalid_companies:
            issues.append({
                'type': 'Invalid Company Reference',
                'customer_id': customer.id,
                'details': f'Company ID {customer.company_id} not found'
            })
        
        # Check address references
        invalid_addresses = self.session.query(Customer).filter(
            (Customer.billing_address_id.isnot(None) & 
             ~Customer.billing_address_id.in_(self.session.query(Address.id))) |
            (Customer.shipping_address_id.isnot(None) & 
             ~Customer.shipping_address_id.in_(self.session.query(Address.id)))
        ).all()
        
        for customer in invalid_addresses:
            if (customer.billing_address_id and 
                not self.session.query(Address).get(customer.billing_address_id)):
                issues.append({
                    'type': 'Invalid Billing Address',
                    'customer_id': customer.id,
                    'details': f'Address ID {customer.billing_address_id} not found'
                })
            
            if (customer.shipping_address_id and 
                not self.session.query(Address).get(customer.shipping_address_id)):
                issues.append({
                    'type': 'Invalid Shipping Address',
                    'customer_id': customer.id,
                    'details': f'Address ID {customer.shipping_address_id} not found'
                })
        
        return issues

    def _get_orphaned_records(self) -> List[Dict[str, Any]]:
        """Get detailed information about orphaned records."""
        orphans = []
        
        # Check for orphaned addresses
        orphaned_addresses = self.session.query(Address).filter(
            ~Address.id.in_(
                self.session.query(Customer.billing_address_id).union(
                    self.session.query(Customer.shipping_address_id)
                )
            )
        ).all()
        
        for address in orphaned_addresses:
            orphans.append({
                'type': 'Orphaned Address',
                'id': address.id,
                'details': f'Address not linked to any customer'
            })
        
        # Check for orphaned emails
        orphaned_emails = self.session.query(CustomerEmail).filter(
            ~CustomerEmail.customer_id.in_(
                self.session.query(Customer.id)
            )
        ).all()
        
        for email in orphaned_emails:
            orphans.append({
                'type': 'Orphaned Email',
                'id': email.id,
                'details': f'Email record not linked to valid customer'
            })
        
        # Check for orphaned phones
        orphaned_phones = self.session.query(CustomerPhone).filter(
            ~CustomerPhone.customer_id.in_(
                self.session.query(Customer.id)
            )
        ).all()
        
        for phone in orphaned_phones:
            orphans.append({
                'type': 'Orphaned Phone',
                'id': phone.id,
                'details': f'Phone record not linked to valid customer'
            })
        
        return orphans


class SalesVerifier(BaseProcessor):
    """Verifies the integrity of imported sales data."""

    def __init__(self, config):
        super().__init__(config)
        self.issues: List[Dict[str, Any]] = []
        self.logger = get_logger(__name__)

    def verify(self, file: Path) -> None:
        """
        Verify data integrity for the given file.
        
        Args:
            file: Path to the file being verified
        """
        with SessionManager(self.config['database_url']) as session:
            self.verify_customer_references(session)
            self.verify_product_references(session)
            self.verify_order_totals(session)
            self.verify_no_orphans(session)
            
            if self.issues:
                self.logger.warning(f"Found {len(self.issues)} issues during verification")
                for issue in self.issues:
                    self.logger.warning(f"{issue['type']}: {issue['message']}")
            else:
                self.logger.info("Verification completed successfully - no issues found")

    def verify_customer_references(self, session) -> None:
        """Verify all orders link to valid customers."""
        orders_without_customers = session.query(Order).filter(
            ~Order.customerId.in_(
                session.query(Customer.id)
            )
        ).all()
        
        if orders_without_customers:
            for order in orders_without_customers:
                self.issues.append({
                    "type": "CRITICAL",
                    "message": f"Order {order.id} references non-existent customer {order.customerId}"
                })

    def verify_product_references(self, session) -> None:
        """Verify all order items link to valid products."""
        items_without_products = session.query(OrderItem).filter(
            ~OrderItem.productCode.in_(
                session.query(Product.productCode)
            )
        ).all()
        
        if items_without_products:
            for item in items_without_products:
                self.issues.append({
                    "type": "CRITICAL",
                    "message": f"Order item {item.id} references non-existent product {item.productCode}"
                })

    def verify_order_totals(self, session) -> None:
        """Verify order totals match sum of line items."""
        orders = session.query(Order).all()
        
        for order in orders:
            items = session.query(OrderItem).filter(OrderItem.orderId == order.id).all()
            items_total = sum(item.quantity * item.unitPrice for item in items)
            if abs(items_total - order.totalAmount) > 0.01:  # Allow for small rounding differences
                self.issues.append({
                    "type": "CRITICAL",
                    "message": f"Order {order.id} total ({order.totalAmount}) does not match sum of line items ({items_total})"
                })

    def verify_no_orphans(self, session) -> None:
        """Verify no orphaned records exist."""
        # Check for order items without orders
        orphaned_items = session.query(OrderItem).filter(
            ~OrderItem.orderId.in_(
                session.query(Order.id)
            )
        ).all()
        
        if orphaned_items:
            for item in orphaned_items:
                self.issues.append({
                    "type": "CRITICAL",
                    "message": f"Orphaned order item found: {item.id}"
                })

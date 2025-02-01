from typing import Dict, List, Optional
from pathlib import Path
from sqlalchemy import select, func
from sqlalchemy.orm import Session

from ..db.models.customer import Customer
from ..db.models.company import Company
from ..db.models.address import Address
from ..db.models.customer_email import CustomerEmail
from ..db.models.customer_phone import CustomerPhone

class ImportVerifier:
    """Verifies data integrity after import process."""
    
    def __init__(self, session: Session):
        self.session = session
        self.summary = {
            'customers': {
                'total': 0,
                'with_company': 0,
                'with_billing_address': 0,
                'with_shipping_address': 0,
                'with_emails': 0,
                'with_phones': 0
            },
            'orphaned': {
                'addresses': 0,
                'emails': 0,
                'phones': 0
            },
            'relationships': {
                'invalid_company_refs': 0,
                'invalid_address_refs': 0
            }
        }

    def verify_customer_relationships(self) -> List[Dict]:
        """Verify all customer relationships are valid."""
        issues = []
        
        # Get all customers
        customers = self.session.execute(select(Customer)).scalars().all()
        self.summary['customers']['total'] = len(customers)
        
        for customer in customers:
            # Check company relationship
            if customer.companyDomain:
                company = self.session.execute(
                    select(Company).where(Company.domain == customer.companyDomain)
                ).scalar_one_or_none()
                
                if company:
                    self.summary['customers']['with_company'] += 1
                else:
                    self.summary['relationships']['invalid_company_refs'] += 1
                    issues.append({
                        'type': 'invalid_company',
                        'customer_id': customer.id,
                        'company_domain': customer.companyDomain
                    })
            
            # Check address relationships
            if customer.billingAddressId:
                address = self.session.execute(
                    select(Address).where(Address.id == customer.billingAddressId)
                ).scalar_one_or_none()
                
                if address:
                    self.summary['customers']['with_billing_address'] += 1
                else:
                    self.summary['relationships']['invalid_address_refs'] += 1
                    issues.append({
                        'type': 'invalid_billing_address',
                        'customer_id': customer.id,
                        'address_id': customer.billingAddressId
                    })
            
            if customer.shippingAddressId:
                address = self.session.execute(
                    select(Address).where(Address.id == customer.shippingAddressId)
                ).scalar_one_or_none()
                
                if address:
                    self.summary['customers']['with_shipping_address'] += 1
                else:
                    self.summary['relationships']['invalid_address_refs'] += 1
                    issues.append({
                        'type': 'invalid_shipping_address',
                        'customer_id': customer.id,
                        'address_id': customer.shippingAddressId
                    })
            
            # Check for contact information
            has_emails = self.session.execute(
                select(func.count()).select_from(CustomerEmail).where(
                    CustomerEmail.customerId == customer.id
                )
            ).scalar_one()
            
            if has_emails:
                self.summary['customers']['with_emails'] += 1
            
            has_phones = self.session.execute(
                select(func.count()).select_from(CustomerPhone).where(
                    CustomerPhone.customerId == customer.id
                )
            ).scalar_one()
            
            if has_phones:
                self.summary['customers']['with_phones'] += 1
        
        return issues

    def find_orphaned_records(self) -> List[Dict]:
        """Find records without valid parent relationships."""
        orphans = []
        
        # Find orphaned addresses
        orphaned_addresses = self.session.execute(
            select(Address).where(
                ~Address.id.in_(
                    select(Customer.billingAddressId).where(Customer.billingAddressId.is_not(None))
                ) &
                ~Address.id.in_(
                    select(Customer.shippingAddressId).where(Customer.shippingAddressId.is_not(None))
                )
            )
        ).scalars().all()
        
        self.summary['orphaned']['addresses'] = len(orphaned_addresses)
        for addr in orphaned_addresses:
            orphans.append({
                'type': 'orphaned_address',
                'id': addr.id,
                'details': f"{addr.line1}, {addr.city}, {addr.state}"
            })
        
        # Find orphaned emails
        orphaned_emails = self.session.execute(
            select(CustomerEmail).where(
                ~CustomerEmail.customerId.in_(
                    select(Customer.id)
                )
            )
        ).scalars().all()
        
        self.summary['orphaned']['emails'] = len(orphaned_emails)
        for email in orphaned_emails:
            orphans.append({
                'type': 'orphaned_email',
                'id': email.id,
                'details': email.email
            })
        
        # Find orphaned phones
        orphaned_phones = self.session.execute(
            select(CustomerPhone).where(
                ~CustomerPhone.customerId.in_(
                    select(Customer.id)
                )
            )
        ).scalars().all()
        
        self.summary['orphaned']['phones'] = len(orphaned_phones)
        for phone in orphaned_phones:
            orphans.append({
                'type': 'orphaned_phone',
                'id': phone.id,
                'details': phone.phone
            })
        
        return orphans

    def verify_import(self) -> Dict:
        """Run all verification checks and return results."""
        relationship_issues = self.verify_customer_relationships()
        orphaned_records = self.find_orphaned_records()
        
        return {
            'summary': self.summary,
            'relationship_issues': relationship_issues,
            'orphaned_records': orphaned_records,
            'success': len(relationship_issues) == 0 and len(orphaned_records) == 0
        }

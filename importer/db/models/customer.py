"""Customer model for storing customer information."""
from datetime import datetime
import uuid
from sqlalchemy import Column, String, DateTime, ForeignKey, JSON
from sqlalchemy.orm import relationship
from .base import Base

class Customer(Base):
    """Customer model."""
    
    __tablename__ = 'Customer'
    
    id = Column(String, primary_key=True)
    customerName = Column(String, nullable=False)
    companyDomain = Column(String, ForeignKey('Company.domain'))
    quickbooksId = Column(String)
    status = Column(String, nullable=False, default='ACTIVE')
    terms = Column(String)
    taxCode = Column(String)
    taxItem = Column(String)
    resaleNumber = Column(String)
    creditLimit = Column(String)
    billingAddressId = Column(String, ForeignKey('Address.id'))
    shippingAddressId = Column(String, ForeignKey('Address.id'))
    sourceData = Column(JSON, nullable=False, default=dict)
    createdAt = Column(DateTime, nullable=False, default=datetime.utcnow)
    modifiedAt = Column(DateTime)

    # Relationships
    emails = relationship("CustomerEmail", back_populates="customer", cascade="all, delete-orphan")
    phones = relationship("CustomerPhone", back_populates="customer", cascade="all, delete-orphan")
    
    @classmethod
    def create(cls, name: str, quickbooks_id: str, company_domain: str, 
               billing_address_id: str = None, shipping_address_id: str = None) -> 'Customer':
        """Create a new customer record."""
        now = datetime.utcnow()
        return cls(
            id=str(uuid.uuid4()),
            customerName=name,
            quickbooksId=quickbooks_id,
            companyDomain=company_domain.lower(),
            status='ACTIVE',  # Default status
            billingAddressId=billing_address_id,
            shippingAddressId=shipping_address_id,
            createdAt=now,
            modifiedAt=now,  # Set initial modified time to creation time
            sourceData={}  # Empty JSON object as default
        )
    
    def __repr__(self):
        """String representation."""
        return f"<Customer(name='{self.customerName}', quickbooks_id='{self.quickbooksId}', company_domain='{self.companyDomain}')>"

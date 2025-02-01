"""Order model definition."""

import enum
from sqlalchemy import Column, String, DateTime, Numeric, ForeignKey, Enum
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import JSONB

from .base import Base

class OrderStatus(enum.Enum):
    """Order status enum."""
    OPEN = 'OPEN'
    CLOSED = 'CLOSED'

class PaymentStatus(enum.Enum):
    """Payment status enum."""
    UNPAID = 'UNPAID'
    PAID = 'PAID'

class Order(Base):
    """Order model."""
    
    __tablename__ = 'Order'
    
    id = Column(String, primary_key=True)
    orderNumber = Column(String, unique=True, nullable=False)
    customerId = Column(String, ForeignKey('Customer.id'), nullable=False)
    orderDate = Column(DateTime, nullable=False)
    status = Column(Enum(OrderStatus), nullable=False)
    paymentStatus = Column(Enum(PaymentStatus), nullable=False)
    subtotal = Column(Numeric, nullable=False)
    taxPercent = Column(Numeric)
    taxAmount = Column(Numeric)
    totalAmount = Column(Numeric, nullable=False)
    billingAddressId = Column(String, ForeignKey('Address.id'))
    shippingAddressId = Column(String, ForeignKey('Address.id'))
    paymentMethod = Column(String)
    paymentDate = Column(DateTime)
    terms = Column(String)
    dueDate = Column(DateTime)
    poNumber = Column(String)
    class_ = Column('class', String)  # Using class_ since class is a Python keyword
    shippingMethod = Column(String)
    shipDate = Column(DateTime)
    quickbooksId = Column(String)
    sourceData = Column(JSONB)
    createdAt = Column(DateTime, nullable=False, server_default=func.now())
    modifiedAt = Column(DateTime)
    
    def __repr__(self):
        """Return string representation."""
        return f'<Order(id="{self.id}", number="{self.orderNumber}", customer="{self.customerId}")>'

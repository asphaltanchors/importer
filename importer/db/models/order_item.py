"""OrderItem model definition."""

from sqlalchemy import Column, String, DateTime, Numeric, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB

from .base import Base

class OrderItem(Base):
    """OrderItem model."""
    
    __tablename__ = 'OrderItem'
    
    id = Column(String, primary_key=True)
    orderId = Column(String, ForeignKey('Order.id'), nullable=False)
    productCode = Column(String, ForeignKey('Product.productCode'), nullable=False)
    description = Column(String)
    quantity = Column(Numeric, nullable=False)
    unitPrice = Column(Numeric, nullable=False)
    amount = Column(Numeric, nullable=False)
    serviceDate = Column(DateTime)
    sourceData = Column(JSONB)
    
    def __repr__(self):
        """Return string representation."""
        return f'<OrderItem(id="{self.id}", order="{self.orderId}", product="{self.productCode}")>'

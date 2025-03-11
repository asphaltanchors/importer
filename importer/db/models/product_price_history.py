"""Product price history model definition."""

from sqlalchemy import Column, String, DateTime, Numeric, ForeignKey
from sqlalchemy.orm import relationship

from .base import Base

class ProductPriceHistory(Base):
    """Product price history model."""
    
    __tablename__ = 'ProductPriceHistory'  # SQLAlchemy will handle proper quoting
    
    id = Column(String, primary_key=True)
    productId = Column(String, ForeignKey('Product.id'), nullable=False)
    cost = Column(Numeric(10, 2), nullable=False)
    listPrice = Column(Numeric(10, 2), nullable=False)
    effectiveDate = Column(DateTime, nullable=False)
    notes = Column(String)
    
    # Relationship
    product = relationship("Product")
    
    def __repr__(self):
        """Return string representation."""
        return f'<ProductPriceHistory(id="{self.id}", productId="{self.productId}", effectiveDate="{self.effectiveDate}")>'

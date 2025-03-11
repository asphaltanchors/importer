"""Product model definition."""

from sqlalchemy import Column, String, DateTime, Numeric, Integer
from sqlalchemy.sql import func

from .base import Base

class Product(Base):
    """Product model."""
    
    __tablename__ = 'Product'  # SQLAlchemy will handle proper quoting
    
    id = Column(String, primary_key=True)
    productCode = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=False)
    description = Column(String)
    createdAt = Column(DateTime, nullable=False)
    modifiedAt = Column(DateTime, nullable=False)
    cost = Column(Numeric(10, 2))
    listPrice = Column(Numeric(10, 2))
    unitsPerPackage = Column(Integer, default=6, nullable=False)
    
    def __repr__(self):
        """Return string representation."""
        return f'<Product(id="{self.id}", code="{self.productCode}", name="{self.name}")>'

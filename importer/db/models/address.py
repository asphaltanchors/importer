"""Address model for storing customer billing and shipping addresses."""
from sqlalchemy import Column, Text
from .base import Base

class Address(Base):
    """SQLAlchemy model for the Address table."""
    
    __tablename__ = 'Address'

    id = Column(Text, primary_key=True)
    line1 = Column(Text, nullable=False)
    line2 = Column(Text)
    line3 = Column(Text)
    city = Column(Text, nullable=False)
    state = Column(Text, nullable=False)
    postalCode = Column(Text, nullable=False)
    country = Column(Text, nullable=False)

    def __repr__(self):
        """String representation of the address."""
        return f"<Address(id='{self.id}', line1='{self.line1}', city='{self.city}', state='{self.state}')>"

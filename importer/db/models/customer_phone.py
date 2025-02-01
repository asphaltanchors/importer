from sqlalchemy import Column, String, Boolean, ForeignKey, Enum as SQLEnum
from sqlalchemy.dialects.postgresql import ENUM
from sqlalchemy.orm import relationship

from .base import Base

class CustomerPhone(Base):
    """Model representing customer phone numbers."""
    __tablename__ = "CustomerPhone"

    id = Column(String, primary_key=True)
    customerId = Column(String, ForeignKey("Customer.id"), nullable=False)
    phone = Column(String, nullable=False)
    type = Column(ENUM('MAIN', 'MOBILE', 'WORK', 'OTHER', name='PhoneType', create_type=False), nullable=False)
    isPrimary = Column(Boolean, default=False)

    # Relationships
    customer = relationship("Customer", back_populates="phones")

    def __repr__(self):
        return f"<CustomerPhone(id={self.id}, phone={self.phone}, type={self.type}, isPrimary={self.isPrimary})>"

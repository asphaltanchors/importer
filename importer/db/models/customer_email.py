from sqlalchemy import Column, String, Boolean, ForeignKey, Enum as SQLEnum
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import ENUM

from .base import Base

class CustomerEmail(Base):
    """Model representing customer email addresses."""
    __tablename__ = "CustomerEmail"

    id = Column(String, primary_key=True)
    customerId = Column(String, ForeignKey("Customer.id"), nullable=False)
    email = Column(String, nullable=False)
    type = Column(ENUM('MAIN', 'CC', name='EmailType', create_type=False), nullable=False)
    isPrimary = Column(Boolean, default=False)

    # Relationships
    customer = relationship("Customer", back_populates="emails")

    def __repr__(self):
        return f"<CustomerEmail(id={self.id}, email={self.email}, type={self.type}, isPrimary={self.isPrimary})>"

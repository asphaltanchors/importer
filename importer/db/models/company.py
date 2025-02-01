"""Company model for storing company information."""
from datetime import datetime
import uuid
from sqlalchemy import Column, String, Boolean, DateTime, JSON
from .base import Base

class Company(Base):
    """Company model."""
    
    __tablename__ = 'Company'
    
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    domain = Column(String, unique=True, nullable=False)
    enriched = Column(Boolean, default=False)
    enrichedAt = Column(DateTime)
    enrichedSource = Column(String)
    enrichmentError = Column(String)
    enrichmentData = Column(JSON)
    createdAt = Column(DateTime, nullable=False, default=datetime.utcnow)
    
    @classmethod
    def create_from_domain(cls, domain: str) -> 'Company':
        """Create a new company record from a domain."""
        return cls(
            id=str(uuid.uuid4()),
            name=domain.split('.')[0].title(),  # Simple name from domain
            domain=domain.lower()
        )
    
    def __repr__(self):
        """String representation."""
        return f"<Company(domain='{self.domain}', name='{self.name}')>"

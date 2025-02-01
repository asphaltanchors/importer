"""Database session management."""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

class SessionManager:
    """Manages database sessions."""
    
    def __init__(self, database_url: str):
        """Initialize session manager with database URL."""
        self.engine = create_engine(database_url)
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine
        )
        
    def get_session(self) -> Session:
        """Get a new database session."""
        return self.SessionLocal()
        
    def __enter__(self) -> Session:
        """Context manager entry."""
        self.session = self.get_session()
        return self.session
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        try:
            if exc_type is None:
                self.session.commit()
            else:
                self.session.rollback()
        finally:
            self.session.close()

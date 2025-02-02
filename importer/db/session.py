"""Database session management."""
import logging
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
        self.logger = logging.getLogger(__name__)
        
    def get_session(self) -> Session:
        """Get a new database session."""
        session = self.SessionLocal()
        self.logger.debug(f"Created new session: {id(session)}")
        return session
        
    def __enter__(self) -> Session:
        """Context manager entry."""
        self.session = self.get_session()
        self.logger.debug(f"Entering context with session: {id(self.session)}")
        return self.session
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.logger.debug(f"Exiting context with session: {id(self.session)}")
        try:
            if exc_type is None:
                self.logger.debug("Committing session")
                self.session.commit()
            else:
                self.logger.debug("Rolling back session")
                self.session.rollback()
        finally:
            self.logger.debug("Closing session")
            self.session.close()

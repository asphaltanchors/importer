"""
Base command infrastructure for the importer CLI.
Provides common functionality and utilities for all commands.
"""

import click
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from .config import Config

from ..processors.error_tracker import ErrorTracker
import time

class BaseCommand(ABC):
    """Base class for all CLI commands."""
    
    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.error_tracker = ErrorTracker()
        self._session_factory = None
        
        # Get debug status from click context
        ctx = click.get_current_context(silent=True)
        self.debug = bool(ctx and ctx.obj.get('debug'))
        if self.debug:
            self.logger.debug(f"Debug mode enabled for {self.__class__.__name__}")
    
    @property
    def session_factory(self) -> sessionmaker:
        """Get or create SQLAlchemy session factory."""
        if self._session_factory is None:
            if self.debug:
                self.logger.debug(f"Creating new engine for {self.config.database_url}")
            engine = create_engine(self.config.database_url)
            self._session_factory = sessionmaker(bind=engine)
        return self._session_factory
    
    def get_session(self) -> Session:
        """Create a new database session."""
        session = self.session_factory()
        if self.debug:
            session_id = id(session)
            self.logger.debug(f"Created new session {session_id}")
            
            # Wrap session methods to track timing
            original_commit = session.commit
            original_rollback = session.rollback
            original_close = session.close
            
            def timed_commit():
                start = time.time()
                try:
                    original_commit()
                    if self.debug:
                        self.logger.debug(f"Session {session_id} commit completed in {time.time() - start:.3f}s")
                except Exception as e:
                    if self.debug:
                        self.logger.debug(f"Session {session_id} commit failed: {str(e)}")
                    raise
            
            def timed_rollback():
                if self.debug:
                    self.logger.debug(f"Session {session_id} rolling back")
                original_rollback()
            
            def timed_close():
                if self.debug:
                    self.logger.debug(f"Session {session_id} closing")
                original_close()
            
            session.commit = timed_commit
            session.rollback = timed_rollback
            session.close = timed_close
            
        return session
    
    @abstractmethod
    def execute(self) -> None:
        """Execute the command. Must be implemented by subclasses."""
        pass
    
    def validate(self) -> bool:
        """Validate command configuration and requirements.
        
        Returns:
            bool: True if validation passes, False otherwise
        """
        if self.debug:
            self.logger.debug("Validating command configuration")
        return True

class FileInputCommand(BaseCommand):
    """Base class for commands that process input files."""
    
    def __init__(self, config: Config, input_file: Path, output_file: Optional[Path] = None):
        super().__init__(config)
        self.input_file = input_file
        self.output_file = output_file
    
    def validate(self) -> bool:
        """Validate input file exists and is readable."""
        if not super().validate():
            return False
            
        if not self.input_file.exists():
            self.logger.error(f"Input file not found: {self.input_file}")
            return False
            
        if not self.input_file.is_file():
            self.logger.error(f"Input path is not a file: {self.input_file}")
            return False
            
        return True

class DirectoryInputCommand(BaseCommand):
    """Base class for commands that process input directories."""
    
    def __init__(self, config: Config, input_dir: Path):
        super().__init__(config)
        self.input_dir = input_dir
    
    def validate(self) -> bool:
        """Validate input directory exists and is readable."""
        if not super().validate():
            return False
            
        if not self.input_dir.exists():
            self.logger.error(f"Input directory not found: {self.input_dir}")
            return False
            
        if not self.input_dir.is_dir():
            self.logger.error(f"Input path is not a directory: {self.input_dir}")
            return False
            
        return True

def command_error_handler(f):
    """Decorator to handle command execution errors consistently."""
    def wrapper(self, *args, **kwargs):
        try:
            if self.debug:
                self.logger.debug(f"Starting command execution: {f.__name__}")
                start = time.time()
            
            result = f(self, *args, **kwargs)
            
            if self.debug:
                self.logger.debug(f"Command completed in {time.time() - start:.3f}s")
            
            return result
            
        except Exception as e:
            self.error_tracker.add_error(
                'COMMAND_EXECUTION_ERROR',
                f"Command failed: {str(e)}",
                {
                    'command': f.__name__,
                    'args': str(args),
                    'kwargs': str(kwargs),
                    'error': str(e)
                }
            )
            if self.debug:
                self.logger.debug(f"Command failed with error: {str(e)}", exc_info=True)
            raise click.Abort()
    return wrapper

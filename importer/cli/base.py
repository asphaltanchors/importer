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

class BaseCommand(ABC):
    """Base class for all CLI commands."""
    
    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self._session_factory = None
        
        # Set debug level if root logger is in debug mode
        root_logger = logging.getLogger()
        if root_logger.getEffectiveLevel() == logging.DEBUG:
            self.logger.setLevel(logging.DEBUG)
            self.logger.debug(f"Debug logging enabled for {self.__class__.__name__}")
    
    @property
    def session_factory(self) -> sessionmaker:
        """Get or create SQLAlchemy session factory."""
        if self._session_factory is None:
            engine = create_engine(self.config.database_url)
            self._session_factory = sessionmaker(bind=engine)
        return self._session_factory
    
    def get_session(self) -> Session:
        """Create a new database session."""
        return self.session_factory()
    
    @abstractmethod
    def execute(self) -> None:
        """Execute the command. Must be implemented by subclasses."""
        pass
    
    def validate(self) -> bool:
        """Validate command configuration and requirements.
        
        Returns:
            bool: True if validation passes, False otherwise
        """
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
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            logger = logging.getLogger(f.__module__)
            logger.error(f"Command failed: {str(e)}", exc_info=True)
            raise click.Abort()
    return wrapper

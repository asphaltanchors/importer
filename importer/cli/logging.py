"""
Logging configuration for the importer CLI.
Provides consistent logging setup across all commands.
"""

import logging
import sys
from pathlib import Path
from typing import Optional, List
from datetime import datetime

class DebugFormatter(logging.Formatter):
    """Custom formatter for debug output."""
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record with timestamp and source."""
        # Add color if output is to terminal
        if sys.stderr.isatty():  # Check if output is to terminal
            cyan = '\033[0;36m'
            reset = '\033[0m'
            return f"{cyan}[{record.created:.3f}] {record.name}: {record.getMessage()}{reset}"
        return f"[{record.created:.3f}] {record.name}: {record.getMessage()}"

def setup_logging(debug: bool = False) -> None:
    """Setup logging configuration.
    
    Args:
        debug: Enable debug logging
    """
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG if debug else logging.INFO)
    
    # Clear any existing handlers
    root_logger.handlers.clear()
    
    # Add console handler with debug formatter
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(DebugFormatter())
    root_logger.addHandler(console_handler)
    
    # Always keep SQLAlchemy logging at WARNING level
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
    logging.getLogger('sqlalchemy.pool').setLevel(logging.WARNING)

def get_logger(name: str) -> logging.Logger:
    """Get a logger with the specified name.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        logging.Logger: Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Ensure handler uses debug formatter
    if logger.handlers:
        for handler in logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                handler.setFormatter(DebugFormatter())
    
    return logger

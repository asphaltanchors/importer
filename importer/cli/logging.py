"""
Logging configuration for the importer CLI.
Provides consistent logging setup across all commands.
"""

import logging
import sys
from pathlib import Path
from typing import Optional, List
from datetime import datetime

class ColoredFormatter(logging.Formatter):
    """Custom formatter adding colors to log levels."""
    
    COLORS = {
        'DEBUG': '\033[0;36m',    # Cyan
        'INFO': '\033[0;32m',     # Green
        'WARNING': '\033[0;33m',  # Yellow
        'ERROR': '\033[0;31m',    # Red
        'CRITICAL': '\033[0;37;41m',  # White on Red
        'RESET': '\033[0m'        # Reset
    }
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record with colors."""
        # Add color if output is to terminal
        if sys.stderr.isatty():  # Check if output is to terminal
            record.levelname = (
                f"{self.COLORS.get(record.levelname, '')}"
                f"{record.levelname}"
                f"{self.COLORS['RESET']}"
            )
        return super().format(record)

def setup_logging(
    log_dir: Optional[Path] = None,
    level: str = 'INFO',
    log_file_prefix: Optional[str] = None,
    handlers: Optional[List[logging.Handler]] = None
) -> None:
    """Setup logging configuration.
    
    Args:
        log_dir: Optional directory for log files
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file_prefix: Optional prefix for log file names
        handlers: Optional list of additional handlers
    """
    # Create log directory if specified
    if log_dir:
        log_dir.mkdir(exist_ok=True, parents=True)
        
        # Generate log file name with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        prefix = f"{log_file_prefix}_" if log_file_prefix else ""
        log_file = log_dir / f"{prefix}{timestamp}.log"
    
    # Set up formatters
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_formatter = ColoredFormatter(
        '%(levelname)s - %(message)s'
    )
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))
    
    # Clear any existing handlers
    root_logger.handlers.clear()
    
    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    # Add file handler if log directory specified
    if log_dir:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)
    
    # Add any additional handlers
    if handlers:
        for handler in handlers:
            root_logger.addHandler(handler)
    
    # Set library logging levels
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

def get_logger(name: str) -> logging.Logger:
    """Get a logger with the specified name.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        logging.Logger: Configured logger instance
    """
    return logging.getLogger(name)

# ABOUTME: Shared utility functions for all pipeline sources
# ABOUTME: Common data processing, validation, and configuration management functions

import os
import yaml
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List
from pathlib import Path

def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load YAML configuration file
    
    Args:
        config_path: Path to YAML config file
        
    Returns:
        Configuration dictionary
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def setup_logging(source_name: str, log_level: str = "INFO") -> logging.Logger:
    """
    Set up consistent logging for pipeline sources
    
    Args:
        source_name: Name of the data source (e.g., 'quickbooks', 'attio')
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        
    Returns:
        Configured logger instance
    """
    # Create logs directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Configure logger
    logger = logging.getLogger(f"pipeline.{source_name}")
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Remove existing handlers to avoid duplicates
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # File handler for source-specific logs
    file_handler = logging.FileHandler(
        log_dir / f"{source_name}_pipeline.log",
        mode='a'
    )
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    
    # Console handler for immediate feedback
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter(
        '%(levelname)s - %(name)s - %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    return logger

def validate_environment_variables(required_vars: List[str]) -> Dict[str, str]:
    """
    Validate that required environment variables are set
    
    Args:
        required_vars: List of required environment variable names
        
    Returns:
        Dictionary of environment variable values
        
    Raises:
        ValueError: If any required variables are missing
    """
    missing_vars = []
    env_values = {}
    
    for var in required_vars:
        value = os.environ.get(var)
        if not value:
            missing_vars.append(var)
        else:
            env_values[var] = value
    
    if missing_vars:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )
    
    return env_values

def get_current_timestamp() -> str:
    """Get current timestamp in ISO format for load tracking"""
    return datetime.utcnow().isoformat()

def safe_cast_numeric(value: Any, default: float = 0.0) -> float:
    """
    Safely cast a value to numeric, handling common data quality issues
    
    Args:
        value: Value to cast
        default: Default value if casting fails
        
    Returns:
        Numeric value or default
    """
    if value is None or value == '':
        return default
    
    # Handle string values
    if isinstance(value, str):
        # Remove common formatting
        cleaned = value.strip().replace(',', '').replace('$', '').replace('%', '')
        
        # Handle empty after cleaning
        if not cleaned:
            return default
            
        try:
            return float(cleaned)
        except ValueError:
            return default
    
    # Handle numeric values
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

def normalize_string(value: Optional[str]) -> Optional[str]:
    """
    Normalize string values for consistent processing
    
    Args:
        value: String value to normalize
        
    Returns:
        Normalized string or None if empty
    """
    if not value or not isinstance(value, str):
        return None
    
    # Strip whitespace and normalize empty values
    normalized = value.strip()
    if not normalized:
        return None
    
    return normalized

def chunk_list(lst: List[Any], chunk_size: int) -> List[List[Any]]:
    """
    Split list into chunks of specified size
    
    Args:
        lst: List to chunk
        chunk_size: Maximum size of each chunk
        
    Returns:
        List of chunked lists
    """
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]
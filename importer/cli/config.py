"""
Configuration management for the importer CLI.
Handles loading and validating configuration from environment variables and files.
"""

import os
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path
from dotenv import load_dotenv

@dataclass
class Config:
    """Configuration settings for the importer CLI."""
    
    # Database settings
    database_url: str
    
    # Processing settings
    chunk_size: int = 1000
    processor_type: str = 'default'
    
    # Logging settings
    log_level: str = 'INFO'
    log_dir: Optional[Path] = None
    
    # Output settings
    output_format: str = 'text'  # text, json, csv
    
    # Runtime settings
    interactive: bool = True
    dry_run: bool = False
    
    # Performance settings
    batch_size: int = field(default=1000)
    max_workers: int = field(default=1)
    
    @classmethod
    def from_env(cls, env_file: Optional[Path] = None) -> 'Config':
        """Create configuration from environment variables.
        
        Args:
            env_file: Optional path to .env file
            
        Returns:
            Config: Configuration instance
            
        Raises:
            ValueError: If required environment variables are missing
        """
        if env_file:
            load_dotenv(env_file)
        else:
            load_dotenv()
            
        database_url = os.getenv('DATABASE_URL')
        if not database_url:
            raise ValueError("DATABASE_URL environment variable is required")
            
        return cls(
            database_url=database_url,
            chunk_size=int(os.getenv('CHUNK_SIZE', '1000')),
            processor_type=os.getenv('PROCESSOR_TYPE', 'default'),
            log_level=os.getenv('LOG_LEVEL', 'INFO'),
            log_dir=Path(os.getenv('LOG_DIR', 'logs')) if os.getenv('LOG_DIR') else None,
            output_format=os.getenv('OUTPUT_FORMAT', 'text'),
            interactive=os.getenv('INTERACTIVE', 'true').lower() == 'true',
            dry_run=os.getenv('DRY_RUN', 'false').lower() == 'true',
            batch_size=int(os.getenv('BATCH_SIZE', '1000')),
            max_workers=int(os.getenv('MAX_WORKERS', '1'))
        )
    
    def validate(self) -> bool:
        """Validate configuration settings.
        
        Returns:
            bool: True if configuration is valid
        """
        # Validate log directory exists if specified
        if self.log_dir and not self.log_dir.exists():
            try:
                self.log_dir.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                raise ValueError(f"Failed to create log directory: {e}")
        
        # Validate numeric values are positive
        if self.chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if self.max_workers <= 0:
            raise ValueError("max_workers must be positive")
            
        # Validate output format
        valid_formats = ['text', 'json', 'csv']
        if self.output_format not in valid_formats:
            raise ValueError(f"output_format must be one of: {', '.join(valid_formats)}")
            
        return True

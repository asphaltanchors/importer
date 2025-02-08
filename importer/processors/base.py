"""Base processor for CSV imports."""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple, TypeVar, Generic
import logging
import time
import pandas as pd
from sqlalchemy.orm import Session

from ..db.session import SessionManager

class ProcessingStats:
    """Statistics for processing operations."""
    
    def __init__(self):
        """Initialize stats with default values."""
        self._stats = {
            'total_processed': 0,
            'successful_batches': 0,
            'failed_batches': 0,
            'total_errors': 0,
            'processing_time': 0.0,
            'db_operation_time': 0.0,
            'started_at': datetime.utcnow(),
            'completed_at': None
        }
    
    def __getitem__(self, key: str) -> Any:
        """Get stat value by key."""
        return self._stats[key]
    
    def __setitem__(self, key: str, value: Any) -> None:
        """Set stat value by key."""
        self._stats[key] = value
    
    def __getattr__(self, name: str) -> Any:
        """Get stat value by attribute name."""
        try:
            return self._stats[name]
        except KeyError:
            # Create new stat with default value 0
            self._stats[name] = 0
            return 0
    
    def __setattr__(self, name: str, value: Any) -> None:
        """Set stat value by attribute name."""
        if name == '_stats':
            super().__setattr__(name, value)
        else:
            self._stats[name] = value
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert stats to dictionary format."""
        result = {}
        for key, value in self._stats.items():
            if isinstance(value, datetime):
                result[key] = value.isoformat()
            elif isinstance(value, float):
                result[key] = round(value, 3)
            else:
                result[key] = value
        return result

# Generic type for processor-specific configuration
T = TypeVar('T')

class BaseProcessor(ABC, Generic[T]):
    """Abstract base class for CSV processors."""
    
    def __init__(
        self,
        session_manager: SessionManager,
        batch_size: int = 100,
        error_limit: int = 1000,
        debug: bool = False
    ):
        """Initialize processor with session manager and configuration.
        
        Args:
            session_manager: Database session manager
            batch_size: Number of records to process in each batch
            error_limit: Maximum number of errors before stopping
            debug: Enable debug logging
        """
        self.session_manager = session_manager
        self.batch_size = batch_size
        self.error_limit = error_limit
        self.debug = debug
        self.logger = logging.getLogger(self.__class__.__name__)
        self.stats = ProcessingStats()
        
        if self.debug:
            self.logger.debug(f"Initialized {self.__class__.__name__} with batch_size={batch_size}")
    
    @abstractmethod
    def validate_data(self, df: pd.DataFrame) -> Tuple[List[str], List[str]]:
        """Validate data before processing.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (critical_issues, warnings)
        """
        pass
    
    @abstractmethod
    def _process_batch(self, session: Session, batch_df: pd.DataFrame) -> pd.DataFrame:
        """Process a single batch of data.
        
        Args:
            session: Database session for this batch
            batch_df: DataFrame containing the batch data
            
        Returns:
            Processed DataFrame
        """
        pass
    
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """Process the data in batches with error handling and progress tracking.
        
        Args:
            data: DataFrame to process
            
        Returns:
            Processed DataFrame
        """
        if self.debug:
            start_time = time.time()
            self.logger.debug(f"Starting processing of {len(data)} rows")
        
        # Validate data first
        critical_issues, warnings = self.validate_data(data)
        
        # Log warnings but continue
        if warnings:
            self.logger.warning("\nValidation warnings:")
            for warning in warnings:
                self.logger.warning(f"  - {warning}")
            self.logger.warning("Continuing with processing...")
        
        # Stop on critical issues
        if critical_issues:
            self.logger.error("\nData validation failed:")
            for issue in critical_issues:
                self.logger.error(f"  - {issue}")
            self.stats.total_errors += len(critical_issues)
            return pd.DataFrame()
        
        total_rows = len(data)
        total_batches = (total_rows + self.batch_size - 1) // self.batch_size
        result_dfs = []
        
        if self.debug:
            self.logger.debug(f"Processing {total_rows} rows in {total_batches} batches")
        
        for batch_num, start_idx in enumerate(range(0, total_rows, self.batch_size), 1):
            if self.debug:
                batch_start = time.time()
                self.logger.debug(f"\nStarting batch {batch_num}/{total_batches}")
            
            batch_df = data.iloc[start_idx:start_idx + self.batch_size].copy()
            
            try:
                # Process batch with new session
                with self.session_manager as session:
                    if self.debug:
                        session_start = time.time()
                        self.logger.debug(f"Created session {id(session)}")
                    
                    processed_batch = self._process_batch(session, batch_df)
                    result_dfs.append(processed_batch)
                    self.stats.successful_batches += 1
                    
                    if self.debug:
                        session_time = time.time() - session_start
                        self.stats.db_operation_time += session_time
                        self.logger.debug(f"Session operations completed in {session_time:.3f}s")
                
                if self.debug:
                    batch_time = time.time() - batch_start
                    self.stats.processing_time += batch_time
                    self.logger.debug(f"Batch {batch_num} completed in {batch_time:.3f}s")
                
                self.stats.total_processed += len(batch_df)
                
                # Check error limit
                if self.stats.total_errors >= self.error_limit:
                    self.logger.error(f"\nStopping: Error limit ({self.error_limit}) reached")
                    break
                
            except Exception as e:
                self.logger.error(f"\nError in batch {batch_num}:")
                self.logger.error(f"Row index: {start_idx + batch_df.index.get_loc(batch_df.index[0])}")
                self.logger.error(str(e))
                if self.debug:
                    self.logger.debug(f"Failed row data: {batch_df.iloc[0].to_dict()}")
                self.stats.failed_batches += 1
                self.stats.total_errors += 1
                continue
        
        if self.debug:
            total_time = time.time() - start_time
            self.logger.debug("\nProcessing Summary:")
            self.logger.debug(f"Total time: {total_time:.3f}s")
            self.logger.debug(f"Processing time: {self.stats.processing_time:.3f}s")
            self.logger.debug(f"Database operation time: {self.stats.db_operation_time:.3f}s")
        
        self.stats.completed_at = datetime.utcnow()
        
        # Combine all results
        return pd.concat(result_dfs, ignore_index=True) if result_dfs else pd.DataFrame()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get processing statistics."""
        return self.stats.to_dict()

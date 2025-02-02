"""Base processor for CSV imports."""
import logging
from typing import Dict, Any, Optional
import pandas as pd
from sqlalchemy.orm import Session

class BaseProcessor:
    """Base class for CSV processors."""
    
    def __init__(self, session: Optional[Session] = None, batch_size: int = 100):
        """Initialize processor with database session and batch size."""
        self.session = session
        self.batch_size = batch_size
        self.logger = logging.getLogger(self.__class__.__name__)
        self.stats = {
            'total_processed': 0,
            'successful_batches': 0,
            'failed_batches': 0,
            'total_errors': 0
        }
        
    def _process_batch(self, batch_df: pd.DataFrame) -> pd.DataFrame:
        """Process a single batch of data. Must be implemented by subclasses."""
        raise NotImplementedError("Subclasses must implement _process_batch")
        
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """Process the data in batches with error handling and progress tracking."""
        total_batches = (len(data) + self.batch_size - 1) // self.batch_size
        result_dfs = []
        
        for batch_num, start_idx in enumerate(range(0, len(data), self.batch_size), 1):
            batch_df = data.iloc[start_idx:start_idx + self.batch_size].copy()
            
            try:
                # Process batch
                processed_batch = self._process_batch(batch_df)
                if self.session:
                    self.session.commit()
                result_dfs.append(processed_batch)
                self.stats['successful_batches'] += 1
                
                # Log batch progress
                self.logger.debug(f"Batch {batch_num}/{total_batches} ({len(batch_df)} rows)")
                
            except Exception as e:
                # Roll back failed batch
                if self.session:
                    self.session.rollback()
                self.logger.error(f"\nError in batch {batch_num}:")
                self.logger.error(f"Row index: {start_idx + batch_df.index.get_loc(batch_df.index[0])}")
                self.logger.error(str(e))
                self.stats['failed_batches'] += 1
                self.stats['total_errors'] += 1
                # Continue processing remaining batches
                continue
            
            self.stats['total_processed'] += len(batch_df)
        
        # Combine all results
        return pd.concat(result_dfs, ignore_index=True) if result_dfs else pd.DataFrame()
        
    def validate(self, data: pd.DataFrame) -> bool:
        """Validate the data. Override in subclasses."""
        return True
        
    def get_stats(self) -> Dict[str, int]:
        """Get processing statistics."""
        return self.stats

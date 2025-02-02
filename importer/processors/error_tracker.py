"""Error tracking and aggregation for data processors."""

from collections import defaultdict
from typing import Dict, List, Optional, Set
import logging

class ErrorTracker:
    """Track and aggregate errors during processing."""
    
    def __init__(self, max_samples: int = 3):
        """Initialize error tracker.
        
        Args:
            max_samples: Maximum number of samples to store per error type
        """
        self.error_counts = defaultdict(int)
        self.error_samples = defaultdict(list)
        self.max_samples = max_samples
        self.seen_errors: Set[str] = set()  # Track unique error instances
        
    def add_error(self, error_type: str, message: str, context: Optional[Dict] = None) -> None:
        """Add an error occurrence.
        
        Args:
            error_type: Category/type of error
            message: Error message
            context: Optional context data for the error
        """
        # Create unique error key from message
        error_key = f"{error_type}:{message}"
        
        # Only track if we haven't seen this exact error before
        if error_key not in self.seen_errors:
            self.seen_errors.add(error_key)
            self.error_counts[error_type] += 1
            
            # Store sample if we haven't hit limit
            if len(self.error_samples[error_type]) < self.max_samples:
                sample = {
                    'message': message,
                    'context': context or {}
                }
                self.error_samples[error_type].append(sample)
    
    def get_summary(self) -> Dict:
        """Get error summary.
        
        Returns:
            Dict containing error counts and samples
        """
        return {
            'counts': dict(self.error_counts),
            'samples': dict(self.error_samples)
        }
    
    def log_summary(self, logger: logging.Logger) -> None:
        """Log error summary.
        
        Args:
            logger: Logger to use for output
        """
        if not self.error_counts:
            return
            
        logger.warning("\nError Summary:")
        for error_type, count in self.error_counts.items():
            logger.warning(f"\n{error_type} ({count} occurrences):")
            
            # Show samples
            samples = self.error_samples[error_type]
            for i, sample in enumerate(samples, 1):
                logger.warning(f"  Sample {i}:")
                logger.warning(f"    {sample['message']}")
                if sample['context']:
                    for key, value in sample['context'].items():
                        logger.warning(f"    {key}: {value}")

"""Base processor for CSV imports."""
from typing import Dict, Any
import pandas as pd

class BaseProcessor:
    """Base class for CSV processors."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize processor with configuration."""
        self.config = config
        
    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        """Process the data. Override in subclasses."""
        return data
        
    def validate(self, data: pd.DataFrame) -> bool:
        """Validate the data. Override in subclasses."""
        return True

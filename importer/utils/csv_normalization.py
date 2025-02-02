"""CSV column name normalization utilities."""

from typing import List, Dict, Any
import pandas as pd
import numpy as np

def normalize_column_name(name: str) -> str:
    """Normalize a CSV column name.
    
    Normalizes by:
    - Replacing multiple spaces with single space
    - Stripping leading/trailing whitespace
    - Preserving special characters and case
    
    Args:
        name: Raw column name from CSV
        
    Returns:
        Normalized column name
        
    Examples:
        >>> normalize_column_name("Product/Service  Amount")
        "Product/Service Amount"
        >>> normalize_column_name(" Customer Name ")
        "Customer Name"
    """
    return ' '.join(name.split())

def normalize_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize all column names in a DataFrame.
    
    Args:
        df: Input DataFrame with raw column names
        
    Returns:
        DataFrame with normalized column names
        
    Example:
        >>> df = pd.DataFrame(columns=["Product/Service  Amount"])
        >>> normalized = normalize_dataframe_columns(df)
        >>> normalized.columns.tolist()
        ["Product/Service Amount"]
    """
    return df.rename(columns=normalize_column_name)

def normalize_json_value(value: Any) -> Any:
    """Normalize a value for JSON serialization.
    
    Handles:
    - NaN/None -> None
    - numpy types -> Python native types
    - Special numeric cases
    
    Args:
        value: Any value to normalize
        
    Returns:
        JSON serializable value
        
    Examples:
        >>> normalize_json_value(np.nan)
        None
        >>> normalize_json_value(pd.NA)
        None
        >>> normalize_json_value(np.int64(42))
        42
    """
    # Handle NaN-like values
    if pd.isna(value):
        return None
        
    # Convert numpy types to native Python types
    if isinstance(value, (np.integer, np.floating)):
        return value.item()
    
    # Handle other numpy types
    if isinstance(value, np.ndarray):
        return value.tolist()
        
    return value

def validate_json_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure all values in a dict are JSON serializable.
    
    Args:
        data: Dictionary to normalize
        
    Returns:
        Dictionary with all values normalized for JSON serialization
    """
    return {
        k: normalize_json_value(v)
        for k, v in data.items()
    }

def validate_required_columns(df: pd.DataFrame, required_columns: List[str]) -> bool:
    """Validate that all required columns are present after normalization.
    
    Args:
        df: DataFrame with normalized columns
        required_columns: List of required column names
        
    Returns:
        True if all required columns present, False otherwise
        
    Example:
        >>> df = pd.DataFrame(columns=["Product/Service", "Amount"])
        >>> validate_required_columns(df, ["Product/Service", "Amount"])
        True
    """
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        import logging
        logging.warning(f"Missing required columns: {missing}")
        return False
    return True

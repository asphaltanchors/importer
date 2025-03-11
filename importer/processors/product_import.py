"""Product import processor for dedicated product CSV files."""

from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple, Set
import logging
import pandas as pd
import json
from pathlib import Path
from sqlalchemy.orm import Session

from ..utils import generate_uuid
from ..utils.system_products import initialize_system_products, is_system_product
from ..db.models import Product, ProductPriceHistory
from ..db.session import SessionManager
from .base import BaseProcessor
from .error_tracker import ErrorTracker

class ProductImportProcessor(BaseProcessor[Dict[str, Any]]):
    """Process products from a dedicated product CSV file."""
    
    def __init__(
        self,
        config: Dict[str, Any],
        batch_size: int = 100,
        error_limit: int = 1000,
        debug: bool = False,
        track_price_history: bool = True
    ):
        """Initialize the processor.
        
        Args:
            config: Configuration dictionary containing database_url
            batch_size: Number of records to process per batch
            error_limit: Maximum number of errors before stopping
            debug: Enable debug logging
            track_price_history: Whether to track price history changes
        """
        session_manager = SessionManager(config['database_url'])
        super().__init__(session_manager, batch_size, error_limit, debug)
        
        # Track price history option
        self.track_price_history = track_price_history
        
        # Initialize error tracker
        self.error_tracker = ErrorTracker()
        
        # Field mappings from CSV to our schema
        self.field_mappings = {
            'product_code': ['Item Name'],
            'description': ['Sales Description', 'Purchase Description'],
            'cost': ['Purchase Cost'],
            'list_price': ['Sales Price']
        }
        
        # Track processed products across batches
        self.processed_codes: Set[str] = set()
        
        # Add product-specific stats
        self.stats.total_products = 0
        self.stats.created = 0
        self.stats.updated = 0
        self.stats.price_updated = 0
        self.stats.history_entries = 0
        self.stats.skipped = 0
        self.stats.validation_errors = 0
        
        # Initialize system products
        if self.debug:
            self.logger.debug("Initializing system products...")
            
        with self.session_manager as session:
            initialize_system_products(session)
            
        if self.debug:
            self.logger.debug("System products initialized")

    def validate_data(self, df: pd.DataFrame) -> Tuple[List[str], List[str]]:
        """Validate data before processing.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Tuple of (critical_issues, warnings)
        """
        critical_issues = []
        warnings = []
        
        # Check required columns
        required_columns = ['Item Name']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            critical_issues.append(f"Missing required columns: {', '.join(missing_columns)}")
            return critical_issues, warnings
        
        # Check for empty product codes
        empty_products = df[df['Item Name'].isna()]
        if not empty_products.empty:
            msg = (f"Found {len(empty_products)} rows with missing product codes that will be skipped. "
                  f"First few row numbers: {', '.join(map(str, empty_products.index[:3]))}")
            warnings.append(msg)
        
        # Check for numeric cost and price
        for col_name, field_name in [('Purchase Cost', 'cost'), ('Sales Price', 'list_price')]:
            if col_name in df.columns:
                non_numeric = df[~df[col_name].astype(str).str.replace('.', '', 1).str.isnumeric()]
                if not non_numeric.empty:
                    msg = (f"Found {len(non_numeric)} rows with non-numeric {field_name} values that may cause issues. "
                          f"First few: {', '.join(non_numeric['Item Name'].head(3).tolist())}")
                    warnings.append(msg)
        
        return critical_issues, warnings
        
    def _process_batch(self, session: Session, batch_df: pd.DataFrame) -> pd.DataFrame:
        """Process a batch of product rows.
        
        Args:
            session: Database session for this batch
            batch_df: DataFrame containing batch of rows to process
            
        Returns:
            Processed DataFrame
        """
        if self.debug:
            self.logger.debug(f"Processing batch of {len(batch_df)} rows")
            
        # Map CSV headers to our standardized field names
        header_mapping = {}
        for std_field, possible_names in self.field_mappings.items():
            for name in possible_names:
                if name in batch_df.columns:
                    header_mapping[std_field] = name
                    if self.debug:
                        self.logger.debug(f"Mapped {std_field} -> {name}")
                    break
        
        if 'product_code' not in header_mapping:
            raise ValueError("Could not find product code column")
            
        # Process each row
        for idx, row in batch_df.iterrows():
            try:
                # Get and validate product data
                product_code = str(row[header_mapping['product_code']]).strip().upper()
                
                # Skip empty product codes
                if not product_code:
                    self.stats.skipped += 1
                    continue
                
                # Get description if available
                description = None
                if 'description' in header_mapping:
                    description = str(row.get(header_mapping['description'], '')).strip()
                
                # Get cost and list price if available
                cost = None
                if 'cost' in header_mapping:
                    try:
                        cost_str = str(row.get(header_mapping['cost'], '')).strip()
                        if cost_str:
                            cost = float(cost_str)
                    except ValueError:
                        self.error_tracker.add_error(
                            'validation',
                            f"Invalid cost value: {cost_str}",
                            {'row': row.to_dict()}
                        )
                        self.stats.validation_errors += 1
                
                list_price = None
                if 'list_price' in header_mapping:
                    try:
                        price_str = str(row.get(header_mapping['list_price'], '')).strip()
                        if price_str:
                            list_price = float(price_str)
                    except ValueError:
                        self.error_tracker.add_error(
                            'validation',
                            f"Invalid list price value: {price_str}",
                            {'row': row.to_dict()}
                        )
                        self.stats.validation_errors += 1
                
                # Skip duplicates and system products (already initialized)
                if product_code in self.processed_codes:
                    self.stats.skipped += 1
                    continue
                if is_system_product(product_code):
                    self.stats.skipped += 1
                    continue
                self.processed_codes.add(product_code)
                
                self.stats.total_products += 1
                
                # Get or create product
                product = session.query(Product).filter(
                    Product.productCode == product_code
                ).first()
                
                now = datetime.utcnow()
                
                if product:
                    if self.debug:
                        self.logger.debug(f"Found existing product: {product_code}")
                    
                    # Track if price changed
                    price_changed = False
                    
                    # Update fields if provided
                    if description and description != product.description:
                        product.description = description
                        product.modifiedAt = now
                    
                    if cost is not None and (product.cost is None or float(product.cost) != cost):
                        # Price changed, maybe track history
                        price_changed = True
                        product.cost = cost
                        product.modifiedAt = now
                    
                    if list_price is not None and (product.listPrice is None or float(product.listPrice) != list_price):
                        # Price changed, maybe track history
                        price_changed = True
                        product.listPrice = list_price
                        product.modifiedAt = now
                    
                    if price_changed:
                        self.stats.price_updated += 1
                        
                        # Track price history if enabled and prices changed
                        if self.track_price_history:
                            history_entry = ProductPriceHistory(
                                id=generate_uuid(),
                                productId=product.id,
                                cost=product.cost or 0,
                                listPrice=product.listPrice or 0,
                                effectiveDate=now,
                                notes="Updated via product import"
                            )
                            session.add(history_entry)
                            self.stats.history_entries += 1
                            
                        self.stats.updated += 1
                    else:
                        # No changes needed
                        self.stats.skipped += 1
                else:
                    if self.debug:
                        self.logger.debug(f"Creating new product: {product_code}")
                    # Create new product
                    product = Product(
                        id=generate_uuid(),
                        productCode=product_code,
                        name=product_code,  # Use code as name initially
                        description=description,
                        cost=cost,
                        listPrice=list_price,
                        createdAt=now,
                        modifiedAt=now
                    )
                    session.add(product)
                    self.stats.created += 1
                    
                    # Add initial price history entry if tracking enabled
                    if self.track_price_history and (cost is not None or list_price is not None):
                        history_entry = ProductPriceHistory(
                            id=generate_uuid(),
                            productId=product.id,
                            cost=cost or 0,
                            listPrice=list_price or 0,
                            effectiveDate=now,
                            notes="Initial import"
                        )
                        session.add(history_entry)
                        self.stats.history_entries += 1
                    
            except Exception as e:
                self.error_tracker.add_error(
                    'processing',
                    str(e),
                    {'row': row.to_dict()}
                )
                self.stats.total_errors += 1
                if self.debug:
                    self.logger.debug(f"Error processing row: {str(e)}", exc_info=True)
                continue
        
        return batch_df
        
    def process_file(self, file_path: Path) -> Dict[str, Any]:
        """Process a CSV file.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            Dictionary with processing results
        """
        if self.debug:
            self.logger.debug(f"Reading CSV file: {file_path}")
            
        # Read CSV file
        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            error_msg = f"Error reading CSV file: {str(e)}"
            self.logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg,
                'summary': {
                    'stats': self.get_stats()
                }
            }
            
        if self.debug:
            self.logger.debug(f"Read {len(df)} rows from {file_path}")
            
        # Process data
        processed_df = self.process(df)
        
        # Prepare results
        results = {
            'success': True,
            'summary': {
                'stats': self.get_stats(),
                'errors': self.error_tracker.get_summary() if hasattr(self, 'error_tracker') else {}
            }
        }
        
        return results
        
    def get_stats(self) -> Dict[str, Any]:
        """Get processing statistics."""
        return self.stats.to_dict()

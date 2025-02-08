"""Product data processor."""

from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
import logging
import pandas as pd
from sqlalchemy.orm import Session

from ..utils import generate_uuid
from ..utils.system_products import initialize_system_products, is_system_product
from ..db.models import Product
from ..db.session import SessionManager
from .base import BaseProcessor
from .error_tracker import ErrorTracker

class ProductProcessor(BaseProcessor[Dict[str, Any]]):
    """Process products from sales data."""
    
    def __init__(
        self,
        config: Dict[str, Any],
        batch_size: int = 100,
        error_limit: int = 1000,
        debug: bool = False
    ):
        """Initialize the processor.
        
        Args:
            config: Configuration dictionary containing database_url
            batch_size: Number of records to process per batch
            error_limit: Maximum number of errors before stopping
            debug: Enable debug logging
        """
        session_manager = SessionManager(config['database_url'])
        super().__init__(session_manager, batch_size, error_limit, debug)
        
        # Initialize error tracker
        self.error_tracker = ErrorTracker()
        
        # Field mappings from CSV to our schema
        self.field_mappings = {
            'product_code': ['Product/Service'],
            'description': ['Product/Service Description']
        }
        
        # Track processed products across batches
        self.processed_codes: Set[str] = set()
        
        # Add product-specific stats
        self.stats.total_products = 0
        self.stats.created = 0
        self.stats.updated = 0
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
        required_columns = ['Product/Service']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            critical_issues.append(f"Missing required columns: {', '.join(missing_columns)}")
            return critical_issues, warnings
        
        # Check for empty product codes
        empty_products = df[df['Product/Service'].isna()]
        if not empty_products.empty:
            msg = (f"Found {len(empty_products)} rows with missing product codes that will be skipped. "
                  f"First few row numbers: {', '.join(map(str, empty_products.index[:3]))}")
            warnings.append(msg)
        
        # Check for test products
        test_products = df[df['Product/Service'].str.startswith('TEST-', na=False)]
        if not test_products.empty:
            msg = (f"Found {len(test_products)} test products that will be skipped. "
                  f"First few: {', '.join(test_products['Product/Service'].head(3).tolist())}")
            warnings.append(msg)
        
        # Check for deprecated products
        if 'Product/Service Description' in df.columns:
            deprecated = df[df['Product/Service Description'].str.lower().str.startswith('deprecated', na=False)]
            if not deprecated.empty:
                msg = (f"Found {len(deprecated)} deprecated products that will be skipped. "
                      f"First few: {', '.join(deprecated['Product/Service'].head(3).tolist())}")
                warnings.append(msg)
        
        return critical_issues, warnings
        
    def _validate_product_code(self, product_code: str) -> Optional[str]:
        """Validate product code format.
        
        Args:
            product_code: Product code to validate
            
        Returns:
            None if valid, error message if invalid
        """
        if not product_code:
            return "Product code is required"
        if len(product_code) > 50:  # Arbitrary limit
            return "Product code exceeds maximum length"
        # Allow alphanumeric, hyphen, underscore, and period
        if not all(c.isalnum() or c in '-_.' for c in product_code):
            return "Product code contains invalid characters (only letters, numbers, hyphen, underscore, and period allowed)"
        return None
        
    def _validate_description(self, description: str) -> Optional[str]:
        """Validate product description.
        
        Args:
            description: Product description to validate
            
        Returns:
            None if valid, error message if invalid
        """
        if not description:
            return None  # Description is optional
        if len(description) > 500:  # Arbitrary limit
            return "Description exceeds maximum length"
        return None
        
    def _validate_product_data(self, product_code: str, description: str) -> List[str]:
        """Validate all product data fields.
        
        Args:
            product_code: Product code to validate
            description: Product description to validate
            
        Returns:
            List of validation error messages
        """
        errors = []
        
        # Check product code
        code_error = self._validate_product_code(product_code)
        if code_error:
            errors.append(code_error)
            
        # Check description
        desc_error = self._validate_description(description)
        if desc_error:
            errors.append(desc_error)
            
        # Business rules
        if product_code.startswith('TEST-'):
            errors.append("Test products not allowed in production")
            
        if description and description.lower().startswith('deprecated'):
            errors.append("Deprecated products should not be imported")
            
        return errors

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
                description = str(row.get(header_mapping.get('description', ''), '')).strip()
                
                validation_errors = self._validate_product_data(product_code, description)
                if validation_errors:
                    if self.debug:
                        self.logger.debug(f"Validation errors for {product_code}: {validation_errors}")
                    self.stats.validation_errors += len(validation_errors)
                    for error in validation_errors:
                        self.error_tracker.add_error(
                            'validation',
                            error,
                            {'row': row.to_dict()}
                        )
                    continue
                
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
                    # Update existing product
                    if description and description != product.description:
                        product.description = description
                        product.modifiedAt = now
                        self.stats.updated += 1
                        if self.debug:
                            self.logger.debug(f"Updated description for {product_code}")
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
                        createdAt=now,
                        modifiedAt=now
                    )
                    session.add(product)
                    self.stats.created += 1
                    
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

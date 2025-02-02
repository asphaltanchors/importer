"""Product data processor."""

from datetime import datetime
from typing import Dict, Any, List, Set
from pathlib import Path
import logging
import pandas as pd

from ..db.session import SessionManager
from ..utils import generate_uuid
from ..db.models import Product
from .base import BaseProcessor

class ProductProcessor(BaseProcessor):
    """Process products from sales data."""
    
    def __init__(self, session_manager: SessionManager, batch_size: int = 100):
        """Initialize the processor.
        
        Args:
            session_manager: Database session manager
            batch_size: Number of products to process per batch
        """
        self.session_manager = session_manager
        super().__init__(None, batch_size)  # We'll manage sessions ourselves
        
        # Field mappings from CSV to our schema
        self.field_mappings = {
            'product_code': ['Product/Service'],
            'description': ['Product/Service Description']
        }
        
        # Track processed products across batches
        self.processed_codes: Set[str] = set()
        
        # Additional stats
        self.stats.update({
            'total_products': 0,
            'created': 0,
            'updated': 0,
            'skipped': 0
        })
        
    def process_file(self, file_path: Path) -> Dict[str, Any]:
        """Process products from a CSV file.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            Dict containing processing results
        """
        try:
            # Read CSV into DataFrame
            df = pd.read_csv(file_path)
            
            # Map CSV headers to our standardized field names
            header_mapping = {}
            for std_field, possible_names in self.field_mappings.items():
                for name in possible_names:
                    if name in df.columns:
                        header_mapping[std_field] = name
                        break
            
            if not all(field in header_mapping for field in self.field_mappings.keys()):
                raise ValueError("Missing required columns in CSV file")
            
            # Process in batches
            total_batches = (len(df) + self.batch_size - 1) // self.batch_size
            print(f"\nProcessing {len(df)} rows in batches of {self.batch_size}", flush=True)
            
            for batch_num, start_idx in enumerate(range(0, len(df), self.batch_size), 1):
                batch_df = df.iloc[start_idx:start_idx + self.batch_size]
                
                try:
                    with self.session_manager as session:
                        # Process batch
                        for _, row in batch_df.iterrows():
                            self._process_row(row, header_mapping, session)
                        
                        # Commit batch
                        session.commit()
                        self.stats['successful_batches'] += 1
                        
                except Exception as e:
                    self.stats['failed_batches'] += 1
                    self.stats['total_errors'] += 1
                    logging.error(f"Error processing batch {batch_num}: {str(e)}")
                    continue
                
                # Print progress
                print(f"Batch {batch_num}/{total_batches} complete", flush=True)
                print(f"Products: {self.stats['total_products']} total, "
                      f"{self.stats['created']} created, "
                      f"{self.stats['updated']} updated", flush=True)
            
            return {
                'success': self.stats['failed_batches'] == 0,
                'summary': {
                    'stats': self.stats,
                    'errors': []  # Detailed errors logged instead of returned
                }
            }
            
        except Exception as e:
            logging.error(f"Failed to process file: {str(e)}")
            return {
                'success': False,
                'summary': {
                    'stats': self.stats,
                    'errors': [{'message': str(e)}]
                }
            }
    
    def _process_row(self, row: pd.Series, header_mapping: Dict[str, str], session) -> None:
        """Process a single product row."""
        # Get product code
        product_code = str(row[header_mapping['product_code']]).strip().upper()
        if not product_code:
            return
            
        # Skip special items and duplicates
        if (product_code.lower() == 'shipping' or
            'tax' in product_code.lower() or
            'discount' in product_code.lower() or
            'handling' in product_code.lower()):
            return
            
        if product_code in self.processed_codes:
            return
        self.processed_codes.add(product_code)
        
        self.stats['total_products'] += 1
        
        # Get or create product
        product = session.query(Product).filter(
            Product.productCode == product_code
        ).first()
        
        description = str(row[header_mapping['description']]).strip()
        now = datetime.utcnow()
        
        if product:
            # Update existing product
            if description and description != product.description:
                product.description = description
                product.modifiedAt = now
                self.stats['updated'] += 1
            else:
                self.stats['skipped'] += 1
        else:
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
            self.stats['created'] += 1

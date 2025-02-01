"""Product data processor."""

from pathlib import Path
from typing import Dict, Any, List
import csv
from datetime import datetime

from ..db.session import SessionManager
from ..utils import generate_uuid
from ..db.models import Product

class ProductProcessor:
    """Process products from sales data."""
    
    def __init__(self, database_url: str):
        """Initialize the processor.
        
        Args:
            database_url: Database connection URL
        """
        self.session_manager = SessionManager(database_url)
        
        # Field mappings from CSV to our schema
        self.field_mappings = {
            'product_code': ['Product/Service'],
            'description': ['Product/Service Description']
        }
        
    def process(self, file_path: Path) -> Dict[str, Any]:
        """Process products from a CSV file.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            Dict containing processing results with structure:
            {
                'success': bool,
                'summary': {
                    'stats': {
                        'total_products': int,
                        'created': int,
                        'updated': int,
                        'skipped': int
                    },
                    'errors': List[Dict]
                }
            }
        """
        results = {
            'success': True,
            'summary': {
                'stats': {
                    'total_products': 0,
                    'created': 0,
                    'updated': 0,
                    'skipped': 0
                },
                'errors': []
            }
        }
        
        try:
            with open(file_path, 'r') as f:
                reader = csv.DictReader(f)
                
                # Map CSV headers to our standardized field names
                header_mapping = {}
                for std_field, possible_names in self.field_mappings.items():
                    for name in possible_names:
                        if name in reader.fieldnames:
                            header_mapping[std_field] = name
                            break
                
                # Track unique products to avoid duplicate processing
                processed_codes = set()
                
                with self.session_manager as session:
                    for row_num, row in enumerate(reader, start=1):
                        # Skip empty rows and special items
                        product_code = row[header_mapping['product_code']].strip()
                        if not product_code:
                            continue
                            
                        product_code = product_code.upper()
                        if (product_code.lower() == 'shipping' or
                            'tax' in product_code.lower() or
                            'discount' in product_code.lower() or
                            'handling' in product_code.lower()):  # Added handling fee skip
                            continue
                        
                        # Skip if we've already processed this product
                        if product_code in processed_codes:
                            continue
                        processed_codes.add(product_code)
                        
                        results['summary']['stats']['total_products'] += 1
                        
                        try:
                            # Get or create product
                            product = session.query(Product).filter(
                                Product.productCode == product_code
                            ).first()
                            
                            description = row[header_mapping['description']].strip()
                            now = datetime.utcnow()
                            
                            if product:
                                # Update existing product
                                if description and description != product.description:
                                    product.description = description
                                    product.modifiedAt = now
                                    results['summary']['stats']['updated'] += 1
                                else:
                                    results['summary']['stats']['skipped'] += 1
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
                                results['summary']['stats']['created'] += 1
                                
                        except Exception as e:
                            results['success'] = False
                            results['summary']['errors'].append({
                                'row': row_num,
                                'severity': 'ERROR',
                                'message': f"Failed to process product {product_code}: {str(e)}"
                            })
                    
        except Exception as e:
            results['success'] = False
            results['summary']['errors'].append({
                'row': 0,
                'severity': 'CRITICAL',
                'message': f"Failed to process file: {str(e)}"
            })
            
        return results

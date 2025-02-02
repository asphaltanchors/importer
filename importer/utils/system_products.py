"""System product utilities."""

from datetime import datetime
from typing import List, Tuple

from ..db.models import Product
from ..utils import generate_uuid

# System product definitions
SYSTEM_PRODUCTS: List[Tuple[str, str, str]] = [
    ('SYS-SHIPPING', 'Shipping', 'System product for shipping and handling charges'),
    ('SYS-TAX', 'Tax', 'System product for sales tax'),
    ('SYS-NJ-TAX', 'NJ Sales Tax', 'System product for New Jersey sales tax'),
    ('SYS-DISCOUNT', 'Discount', 'System product for discounts')
]

# Product type constants
SHIPPING_CODES = ['SYS-SHIPPING']
TAX_CODES = ['SYS-TAX', 'SYS-NJ-TAX']
DISCOUNT_CODES = ['SYS-DISCOUNT']

def initialize_system_products(session) -> List[Product]:
    """Initialize system products in database.
    
    Args:
        session: Database session
        
    Returns:
        List of created/updated system products
    """
    products = []
    now = datetime.utcnow()
    
    for code, name, description in SYSTEM_PRODUCTS:
        product = session.query(Product).filter(
            Product.productCode == code
        ).first()
        
        if not product:
            product = Product(
                id=generate_uuid(),
                productCode=code,
                name=name,
                description=description,
                createdAt=now,
                modifiedAt=now
            )
            session.add(product)
            products.append(product)
    
    session.commit()
    return products

def is_system_product(product_code: str) -> bool:
    """Check if a product code is a system product.
    
    Args:
        product_code: Product code to check
        
    Returns:
        True if product code is a system product
    """
    return any(code for code, _, _ in SYSTEM_PRODUCTS if code == product_code)

def is_shipping_product(product_code: str) -> bool:
    """Check if a product code is a shipping product.
    
    Args:
        product_code: Product code to check
        
    Returns:
        True if product code is a shipping product
    """
    return product_code in SHIPPING_CODES

def is_tax_product(product_code: str) -> bool:
    """Check if a product code is a tax product.
    
    Args:
        product_code: Product code to check
        
    Returns:
        True if product code is a tax product
    """
    return product_code in TAX_CODES

def is_discount_product(product_code: str) -> bool:
    """Check if a product code is a discount product.
    
    Args:
        product_code: Product code to check
        
    Returns:
        True if product code is a discount product
    """
    return product_code in DISCOUNT_CODES

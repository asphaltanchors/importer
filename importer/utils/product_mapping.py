"""Product code mapping utilities."""

from .system_products import SYSTEM_PRODUCTS

# Shipping terms that indicate a shipping/handling charge
SHIPPING_TERMS = [
    'shipping',
    'handling',
    'fed ex',
    'fedex',
    'ups',
    'ground',
    'truck',
    'freight',
    'delivery',
    'coll',
    'xpo',
    'ward'
]

def map_product_code(product_code: str, description: str = '') -> str:
    """Map raw product codes to system product codes.
    
    Args:
        product_code: Raw product code from input data
        description: Optional product description for additional context
        
    Returns:
        Mapped system product code
    """
    product_code_lower = product_code.lower()
    description_lower = description.lower()

    # Check both product code and description for shipping terms
    text_to_check = f"{product_code_lower} {description_lower}"
    if any(term in text_to_check for term in SHIPPING_TERMS):
        return next(code for code, name, _ in SYSTEM_PRODUCTS if name == 'Shipping')
    
    # Tax variations
    elif product_code_lower == 'tax':
        return next(code for code, name, _ in SYSTEM_PRODUCTS if name == 'Tax')
    elif 'nj sales tax' in product_code_lower:
        return next(code for code, name, _ in SYSTEM_PRODUCTS if name == 'NJ Sales Tax')
    
    # Discount variations
    elif product_code_lower == 'discount':
        return next(code for code, name, _ in SYSTEM_PRODUCTS if name == 'Discount')
    
    # Default to uppercase original code
    else:
        return product_code.upper()

"""Product code mapping utilities."""

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

    # Check if this is any kind of shipping/handling charge
    shipping_terms = [
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
    
    # Check both product code and description for shipping terms
    text_to_check = f"{product_code_lower} {description_lower}"
    if any(term in text_to_check for term in shipping_terms):
        return 'SYS-SHIPPING'  # All shipping/handling charges map to SYS-SHIPPING
    
    # Tax variations
    elif product_code_lower == 'tax':
        return 'SYS-TAX'
    elif 'nj sales tax' in product_code_lower:
        return 'SYS-NJ-TAX'
    
    # Discount variations
    elif product_code_lower == 'discount':
        return 'SYS-DISCOUNT'
    
    # Default to uppercase original code
    else:
        return product_code.upper()

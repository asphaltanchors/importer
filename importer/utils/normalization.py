"""Customer name normalization utilities.

This module provides functions for normalizing customer names to enable
consistent matching between variations of the same name, while preserving
important notations like percentages and parent/child relationships.
"""

import logging
from typing import List, Optional, Tuple
from tld import get_fld
from tld.exceptions import TldDomainNotFound, TldBadUrl
from sqlalchemy import func
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

def normalize_customer_name(name: str) -> str:
    """Normalize a customer name for consistent matching.

    Applies the following transformations in order:
    1. Convert to uppercase
    2. Handle comma-based individual names (e.g. "Peterson, Chris" -> "CHRIS PETERSON")
       - Only if no special notations present (%, :, parentheses)
    3. Normalize (not remove) business suffixes (e.g. "LLC." -> "LLC")
    4. Remove extra whitespace
    5. Case-insensitive special notation preservation

    Preserves:
    - Percentage notations (e.g. "We-Do Equipment 35%A 30% (CC IN NOTES)")
    - Parent/child relationships with colons (e.g. "Fastenal (30%):Fastenal Valparaso Indiana")
    - Parenthetical notes (e.g. "Fastco LLC (40%/35%) SEE NOTES")
    - Special characters and formatting

    Args:
        name: The customer name to normalize

    Returns:
        The normalized version of the name

    Examples:
        >>> normalize_customer_name("Peterson, Chris")
        'CHRIS PETERSON'
        >>> normalize_customer_name("We-Do Equipment 35%A 30% (cc in notes)")
        'WE-DO EQUIPMENT 35%A 30% (CC IN NOTES)'
        >>> normalize_customer_name("Fastenal LLC (30%):Fastenal Valparaso Indiana")
        'FASTENAL LLC (30%):FASTENAL VALPARASO INDIANA'
        >>> normalize_customer_name("Fastco LLC. (40%/35%) see notes")
        'FASTCO LLC (40%/35%) SEE NOTES'
    """
    if not name:
        return name

    logger.debug(f"Normalizing customer name: {name}")
    
    # Convert to uppercase and normalize whitespace
    name = " ".join(name.upper().split())
    logger.debug(f"After initial normalization: {name}")
    
    # Check if name contains special notations that should be preserved
    has_special_notation = any(char in name for char in ['%', ':', '(', ')'])
    logger.debug(f"Has special notation: {has_special_notation}")
    
    # Define common business suffixes and their normalized forms
    suffix_mapping = {
        "LLC.": "LLC",
        "INC.": "INC",
        "CORP.": "CORP",
        "LTD.": "LTD",
        "CO.": "CO",
        "CORPORATION": "CORP",
        "LIMITED": "LTD",
        "INCORPORATED": "INC"
    }
    
    def normalize_suffixes(text: str) -> str:
        """Helper to normalize (not remove) business suffixes."""
        words = text.split()
        if len(words) == 1:  # Preserve single words
            return text
            
        # Process all words to handle multiple suffixes
        normalized_words = []
        for word in words:
            # For special notation sections, keep everything as is
            if any(char in word for char in ['%', ':', '(', ')']):
                normalized_words.append(word)
                continue
                
            # Remove trailing periods and check against mapping
            word_no_period = word.rstrip('.')
            if word_no_period in suffix_mapping:
                normalized_words.append(suffix_mapping[word_no_period])
            else:
                normalized_words.append(word.rstrip('.'))
            
        return " ".join(normalized_words)
    
    # Handle comma-based names (e.g. "Peterson, Chris" -> "CHRIS PETERSON")
    # Only if no special notations are present
    if "," in name and not has_special_notation:
        last, first = name.split(",", 1)
        
        # Split the entire name to find suffixes anywhere
        name_parts = name.split()
        suffixes = []
        
        # First collect all suffixes from anywhere in the name
        for part in name_parts:
            part_no_period = part.rstrip('.')
            if part_no_period in suffix_mapping or part in suffix_mapping.values():
                if part_no_period in suffix_mapping:
                    suffix = suffix_mapping[part_no_period]
                    if suffix not in suffixes:  # Avoid duplicates
                        suffixes.append(suffix)
                elif part not in suffixes:  # Avoid duplicates
                    suffixes.append(part)
        
        # Clean up last and first names by removing suffixes
        last_parts = []
        for part in last.strip().split():
            part_no_period = part.rstrip('.')
            if not (part_no_period in suffix_mapping or part in suffix_mapping.values()):
                last_parts.append(part.rstrip('.'))
                
        first_parts = []
        for part in first.strip().split():
            part_no_period = part.rstrip('.')
            if not (part_no_period in suffix_mapping or part in suffix_mapping.values()):
                first_parts.append(part.rstrip('.'))
        
        # Reconstruct the name with suffixes at the end
        name = f"{' '.join(first_parts)} {' '.join(last_parts)}"
        if suffixes:
            name = f"{name} {' '.join(suffixes)}"
        logger.debug(f"After comma name handling: {name}")
    else:
        # For non-comma names or names with special notations
        name = normalize_suffixes(name)
        logger.debug(f"After suffix normalization: {name}")
    
    return name

def find_customer_by_name(session: Session, name: str) -> Tuple[Optional['Customer'], bool]:
    """Find a customer by name using consistent matching logic.
    
    This function implements the standard customer matching logic to be used
    by all processors. It tries:
    1. Exact case-insensitive match
    2. Normalized name match
    
    Args:
        session: Database session
        name: Customer name to search for
        
    Returns:
        Tuple of (customer, used_normalization) where used_normalization indicates
        if the match was found using name normalization.
    """
    from ..db.models import Customer  # Import here to avoid circular dependency
    
    logger.debug(f"Searching for customer by name: {name}")
    
    # Try case-insensitive exact match first
    customer = session.query(Customer).filter(
        func.lower(Customer.customerName) == name.lower()
    ).first()
    
    if customer:
        logger.debug(f"Found exact case-insensitive match: {customer.customerName}")
        return customer, False
        
    # Try normalized match
    normalized_name = normalize_customer_name(name)
    logger.debug(f"Trying normalized match: {normalized_name}")
    
    customer = session.query(Customer).filter(
        func.lower(Customer.customerName) == normalized_name.lower()
    ).first()
    
    if customer:
        logger.info(f"Found normalized name match: '{name}' -> '{customer.customerName}'")
        return customer, True
        
    logger.debug("No customer found")
    return None, False


def normalize_domain(domain: str) -> Optional[str]:
    """Extract the registrable domain from a full domain name.
    
    This function handles subdomains and country-specific TLDs correctly:
    - foo.bar.com -> bar.com
    - sub.example.co.uk -> example.co.uk
    - app.staging.company.com -> company.com
    
    Args:
        domain: The domain name to normalize
        
    Returns:
        The normalized domain name, or None if the domain is invalid
    """
    if not domain or '@' in domain:
        return None
        
    try:
        # get_fld returns the "first level domain" - the registrable domain
        # without subdomains but including the public suffix
        return get_fld(domain.strip().lower(), fix_protocol=True)
    except (TldDomainNotFound, TldBadUrl):
        return None


def _get_test_cases() -> List[tuple[str, str]]:
    """Get test cases for unit testing.
    
    Returns:
        List of (input, expected_output) tuples
    """
    return [
        # Basic cases
        ("ACME Corp", "ACME CORP"),
        ("acme corp", "ACME CORP"),
        ("  ACME  Corp  ", "ACME CORP"),
        
        # Individual names with commas
        ("Peterson, Chris", "CHRIS PETERSON"),
        ("SMITH, JOHN", "JOHN SMITH"),
        ("Jones,   Bob  ", "BOB JONES"),
        
        # Business suffixes (normalized not removed)
        ("EISEN GROUP LLC.", "EISEN GROUP LLC"),
        ("Test Company Inc.", "TEST COMPANY INC"),
        ("Demo Corp.", "DEMO CORP"),
        ("Sample Ltd.", "SAMPLE LTD"),
        ("Example Co.", "EXAMPLE CO"),
        ("Test Corporation", "TEST CORP"),
        ("Demo Incorporated", "DEMO INC"),
        ("Sample Limited", "SAMPLE LTD"),
        
        # Percentage notations (preserved, case-insensitive)
        ("White Cap 30%:Whitecap Edmonton Canada", "WHITE CAP 30%:WHITECAP EDMONTON CANADA"),
        ("Fastenal LLC (30%):Fastenal Valparaso Indiana", "FASTENAL LLC (30%):FASTENAL VALPARASO INDIANA"),
        ("We-Do Equipment 35%A 30% (cc in notes)", "WE-DO EQUIPMENT 35%A 30% (CC IN NOTES)"),
        ("Fastco LLC (40%/35%) see notes", "FASTCO LLC (40%/35%) SEE NOTES"),
        
        # Parent/child relationships (preserved)
        ("Fastenal LLC:Fastenal Valparaso", "FASTENAL LLC:FASTENAL VALPARASO"),
        ("Parent Corp LLC (30%):Child Location", "PARENT CORP LLC (30%):CHILD LOCATION"),
        ("Main LLC:Sub Division", "MAIN LLC:SUB DIVISION"),
        
        # Parenthetical notes (preserved, case-insensitive)
        ("ABC Company LLC (See Notes)", "ABC COMPANY LLC (SEE NOTES)"),
        ("XYZ Corp LLC (requires po)", "XYZ CORP LLC (REQUIRES PO)"),
        ("123 Inc LLC (special terms)", "123 INC LLC (SPECIAL TERMS)"),
        
        # Complex combinations
        ("Parent LLC (35%):Child LLC (See Notes)", "PARENT LLC (35%):CHILD LLC (SEE NOTES)"),
        ("Main Co. LLC 40%A (Notes):Sub 30%", "MAIN CO LLC 40%A (NOTES):SUB 30%"),
        ("Corp Name LLC (40%) + Sub (30%)", "CORP NAME LLC (40%) + SUB (30%)"),
        
        # Real-world examples from database
        ("shore transit llc", "SHORE TRANSIT LLC"),
        ("EISEN GROUP LLC", "EISEN GROUP LLC"),
        ("Pierce Manufacturing Inc", "PIERCE MANUFACTURING INC"),
        ("Prep Kitchens LLC", "PREP KITCHENS LLC"),
        
        # Special characters (preserved)
        ("Johnson & Sons LLC", "JOHNSON & SONS LLC"),
        ("A-1 Services Inc", "A-1 SERVICES INC"),
        ("B&B Solutions Corp", "B&B SOLUTIONS CORP"),
        
        # Edge cases
        ("", ""),
        (None, None),
        ("LLC", "LLC"),  # Single word that's a suffix
        ("Smith, John LLC", "JOHN SMITH LLC"),  # Comma name with suffix
        ("First, Second: Third", "FIRST, SECOND: THIRD"),  # Preserve comma with colon
        ("Company Name LLC (30%, 40%)", "COMPANY NAME LLC (30%, 40%)"),  # Multiple percentages
        ("Name with, comma: and colon", "NAME WITH, COMMA: AND COLON"),  # Multiple special chars
        
        # Mixed case special notations
        ("company LLC (Important note)", "COMPANY LLC (IMPORTANT NOTE)"),
        ("business inc (check po)", "BUSINESS INC (CHECK PO)"),
        ("store corp (Verify ID)", "STORE CORP (VERIFY ID)"),
        
        # Multiple suffixes (normalize all)
        ("Test Company Inc. LLC.", "TEST COMPANY INC LLC"),
        ("Demo Corp. Ltd.", "DEMO CORP LTD"),
        
        # Special characters with suffixes
        ("A&B Corp.", "A&B CORP"),
        ("X-Y-Z Inc.", "X-Y-Z INC"),
        ("1st Choice Ltd.", "1ST CHOICE LTD")
    ]


def run_tests() -> None:
    """Run test cases and print results."""
    print("\nRunning customer name normalization tests...")
    test_cases = _get_test_cases()
    passed = 0
    failed = 0
    
    for input_name, expected in test_cases:
        try:
            result = normalize_customer_name(input_name)
            if result == expected:
                passed += 1
            else:
                failed += 1
                print(f"\nTest failed:")
                print(f"Input:    {input_name}")
                print(f"Expected: {expected}")
                print(f"Got:      {result}")
        except Exception as e:
            failed += 1
            print(f"\nError processing: {input_name}")
            print(f"Error: {str(e)}")
    
    print(f"\nTest Results:")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Total:  {len(test_cases)}")


if __name__ == "__main__":
    run_tests()

"""Customer name normalization utilities.

This module provides functions for normalizing customer names to enable
consistent matching between variations of the same name, while preserving
important notations like percentages and parent/child relationships.
"""

from typing import List


def normalize_customer_name(name: str) -> str:
    """Normalize a customer name for consistent matching.

    Applies the following transformations in order:
    1. Convert to uppercase
    2. Handle comma-based individual names (e.g. "Peterson, Chris" -> "CHRIS PETERSON")
       - Only if no colon present (preserves percentage notations)
    3. Remove common business suffixes (LLC, INC, CORP, LTD, CO)
    4. Remove extra whitespace

    Preserves:
    - Percentage notations (e.g. "White Cap 30%:Whitecap Edmonton Canada")
    - Parent/child relationships with colons
    - Special characters beyond commas

    Args:
        name: The customer name to normalize

    Returns:
        The normalized version of the name

    Examples:
        >>> normalize_customer_name("Peterson, Chris")
        'CHRIS PETERSON'
        >>> normalize_customer_name("EISEN GROUP LLC")
        'EISEN GROUP'
        >>> normalize_customer_name("White Cap 30%:Whitecap Edmonton Canada")
        'WHITE CAP 30%:WHITECAP EDMONTON CANADA'
        >>> normalize_customer_name("advanced Tri-Star Development LLC")
        'ADVANCED TRI-STAR DEVELOPMENT'
    """
    if not name:
        return name

    # Convert to uppercase and normalize whitespace
    name = " ".join(name.upper().split())
    
    # Define common business suffixes (including with periods)
    suffixes = [
        " LLC", " INC", " CORP", " CORP.", " LTD", " CO", " CO.",
        " CORPORATION", " LIMITED"  # Removed COMPANY since it's too common in real names
    ]
    
    def remove_suffixes(text: str) -> str:
        """Helper to remove business suffixes from a string."""
        words = text.split()
        if len(words) == 1:  # Preserve single words
            return text
            
        filtered_words = []
        for i, word in enumerate(words):
            # Only treat as suffix if it's the last word
            if i == len(words) - 1 and any(word == suffix.strip() or word == suffix.strip(".") for suffix in suffixes):
                continue
            filtered_words.append(word)
        
        return " ".join(filtered_words)
    
    # Handle comma-based names (e.g. "Peterson, Chris" -> "CHRIS PETERSON")
    if "," in name and ":" not in name:  # Avoid splitting percentage notations
        last, first = name.split(",", 1)
        # Remove suffixes from both parts before recombining
        last = remove_suffixes(last.strip())
        first = remove_suffixes(first.strip())
        name = f"{first} {last}"
    else:
        # For non-comma names, just remove suffixes once
        name = remove_suffixes(name)
    
    return name


def _get_test_cases() -> List[tuple[str, str]]:
    """Get test cases for unit testing.
    
    Returns:
        List of (input, expected_output) tuples
    """
    return [
        # Basic cases
        ("ACME Corp", "ACME"),
        ("acme corp", "ACME"),
        ("  ACME  Corp  ", "ACME"),
        
        # Individual names with commas
        ("Peterson, Chris", "CHRIS PETERSON"),
        ("SMITH, JOHN", "JOHN SMITH"),
        ("Jones,   Bob  ", "BOB JONES"),
        
        # Business suffixes
        ("EISEN GROUP LLC", "EISEN GROUP"),
        ("Test Company Inc", "TEST COMPANY"),
        ("Demo Corp.", "DEMO"),
        ("Sample Ltd", "SAMPLE"),
        ("Example Co", "EXAMPLE"),
        
        # Percentage notations (preserved)
        ("White Cap 30%:Whitecap Edmonton Canada", "WHITE CAP 30%:WHITECAP EDMONTON CANADA"),
        ("Fastenal (30%):Fastenal Valparaso Indiana", "FASTENAL (30%):FASTENAL VALPARASO INDIANA"),
        
        # Real-world examples from database
        ("shore transit", "SHORE TRANSIT"),
        ("EISEN GROUP", "EISEN GROUP"),
        ("Pierce Manufacturing", "PIERCE MANUFACTURING"),
        ("Prep Kitchens", "PREP KITCHENS"),
        
        # Special characters (preserved)
        ("Johnson & Sons", "JOHNSON & SONS"),
        ("A-1 Services", "A-1 SERVICES"),
        ("B&B Solutions", "B&B SOLUTIONS"),
        
        # Edge cases
        ("", ""),
        (None, None),
        ("LLC", "LLC"),  # Single word that's a suffix
        ("Smith, John LLC", "JOHN SMITH"),  # Comma name with suffix
        ("First, Second: Third", "FIRST, SECOND: THIRD"),  # Preserve comma with colon
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

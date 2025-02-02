# Customer Name Normalization

## Overview

We need to handle customer name variations between QuickBooks CSV files and our database. The main variations we'll handle are:
- Case differences (e.g., "ACME Corp" vs "Acme Corp")
- Commas (e.g., "Smith, John" vs "John Smith")
- Common business suffixes (e.g., "ACME LLC" vs "ACME")

## Normalization Strategy

Instead of storing normalized versions of names, we'll use a runtime normalization function that is applied both during import and lookup operations. This ensures consistent matching without requiring database schema changes.

## Normalization Rules

The normalize_customer_name() function applies the following rules in order:

1. Convert to uppercase
2. Handle comma-based individual names:
   - Split on comma (e.g., "Peterson, Chris" -> "CHRIS PETERSON")
   - Only if no colon present (preserves percentage notations)
3. Remove common business suffixes: LLC, INC, CORP, LTD, CO
4. Remove extra whitespace
5. Preserve special notations:
   - Percentage notations (e.g., "White Cap 30%:Whitecap Edmonton Canada")
   - Parent/child relationships with colons

## Implementation Example

```python
def normalize_customer_name(name: str) -> str:
    # Convert to uppercase
    name = name.upper()
    
    # Handle comma-based names (e.g. "Peterson, Chris" -> "CHRIS PETERSON")
    if "," in name and ":" not in name:  # Avoid splitting percentage notations
        last, first = name.split(",", 1)
        name = f"{first.strip()} {last.strip()}"
    
    # Remove common business suffixes
    suffixes = [" LLC", " INC", " CORP", " LTD", " CO"]
    for suffix in suffixes:
        if name.endswith(suffix):
            name = name[:-len(suffix)]
    
    # Remove extra whitespace
    name = " ".join(name.split())
    
    return name
```

## Matching Process

When looking up customers during sales/invoice imports:

1. First try exact match on original customerName
2. If not found:
   - Normalize the lookup name using normalize_customer_name()
   - Normalize and compare against each stored customerName
   - Return match if found
3. If still not found, report customer not found error

## Example Transformations

```
Original Name                                  | Normalized Name
---------------------------------------------|----------------
Peterson, Chris                               | CHRIS PETERSON
EISEN GROUP LLC                              | EISEN GROUP
White Cap 30%:Whitecap Edmonton Canada       | WHITE CAP 30%:WHITECAP EDMONTON CANADA
advanced Tri-Star Development LLC            | ADVANCED TRI-STAR DEVELOPMENT
```

## Edge Cases Preserved

We explicitly preserve these variations to maintain data integrity:
- Parent/child relationships with colons
- Percentage notations
- Special characters beyond commas

# Customer Name Normalization

## Overview

We need to handle customer name variations between QuickBooks CSV files and our database. The main variations we handle are:
- Case differences (e.g., "ACME Corp" vs "Acme Corp")
- Commas (e.g., "Smith, John" vs "John Smith")
- Common business suffixes (e.g., "ACME LLC" vs "ACME")

## Implementation Status

✅ Core normalization functionality is implemented in `importer.utils.normalization.normalize_customer_name()`.

## Normalization Strategy

Instead of storing normalized versions of names in the database, we use a runtime normalization function that is applied both during import and lookup operations. This ensures consistent matching without requiring database schema changes.

## Normalization Rules

The normalize_customer_name() function applies these transformations in order:

1. Convert to uppercase and normalize whitespace
2. Handle comma-based individual names:
   - Split on comma (e.g., "Peterson, Chris" -> "CHRIS PETERSON")
   - Only if no colon present (preserves percentage notations)
3. Remove common business suffixes if they appear at the end:
   - LLC, INC, CORP, CORP., LTD, CO, CO., CORPORATION, LIMITED
   - Note: "COMPANY" is not removed as it's too common in real business names
4. Preserve special notations:
   - Percentage notations (e.g., "White Cap 30%:Whitecap Edmonton Canada")
   - Parent/child relationships with colons
   - Special characters (e.g., "&", "-")

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
shore transit                                | SHORE TRANSIT
Pierce Manufacturing                         | PIERCE MANUFACTURING
Smith, John LLC                              | JOHN SMITH
```

## Next Steps

1. Update Customer Processor:
   - Import normalize_customer_name
   - Modify customer lookup to use normalization
   - Add logging for normalized matches

2. Update Invoice Processor:
   - Import normalize_customer_name
   - Apply same lookup pattern for finding customers
   - Add logging when normalized match is found

3. Update Sales Receipt Processor:
   - Mirror changes from invoice processor
   - Ensure consistent customer lookup behavior

4. Add Integration Tests:
   - Test customer import with various name formats
   - Test invoice import with normalized customer matching
   - Test sales receipt import with normalized matching
   - Include edge cases (percentage notation, colons)

5. Manual Testing:
   - Import sample customer list
   - Import invoices with name variations
   - Verify correct matching
   - Document any edge cases found

6. Monitoring Plan:
   - Log when normalized matching is used
   - Track success rate of normalized matches
   - Identify patterns in unmatched customers

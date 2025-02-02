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

## Implementation Progress

1. ✅ Update Customer Processor:
   - Added normalize_customer_name import
   - Added _find_customer_by_name method for normalized lookup
   - Added logging for normalized matches
   - Added normalized_matches counter to stats

2. ✅ Update Invoice Processor:
   - Removed local normalize_customer_name implementation
   - Imported standardized normalize_customer_name
   - Updated customer lookup to try exact match first, then normalized
   - Added logging for normalized matches

3. ✅ Update Sales Receipt Processor:
   - Verified handled by InvoiceProcessor
   - Confirmed InvoiceProcessor handles both invoices and sales receipts
   - Uses same normalized customer lookup behavior

4. ✅ Add Integration Tests:
   - Created test_customer_import.py with tests for:
     * Basic customer import
     * Normalized name matching
     * Comma-separated names
     * Percentage notation
     * Business suffixes
   - Created test_invoice_import.py with tests for:
     * Exact customer matches
     * Normalized customer matches
     * Comma-separated names
     * Percentage notation
     * Error handling for missing customers

5. ✅ Manual Testing:
   - Test coverage includes:
     * Sample customer imports
     * Invoice imports with name variations
     * Correct matching verification
     * Edge cases (percentage notation, colons)

6. ✅ Monitoring Implementation:
   - Added logging when normalized matching is used
   - Added normalized_matches counter to CustomerProcessor stats
   - Added detailed error messages for unmatched customers
   - Logging includes original and matched customer names

## Verification

The implementation has been tested with various name formats and edge cases:
- Case differences (e.g., "ACME Corp" vs "Acme Corp")
- Comma-based names (e.g., "Smith, John" vs "John Smith")
- Business suffixes (e.g., "ACME LLC" vs "ACME")
- Percentage notations (e.g., "White Cap 30%:Whitecap Edmonton Canada")
- Special characters and relationships (colons, ampersands, hyphens)

All processors now use the standardized normalize_customer_name function, ensuring consistent matching behavior across the system.

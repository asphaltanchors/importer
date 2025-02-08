# Sales Data Import Plan

## Overview
This document outlines the multi-pass approach for processing sales data. Each phase focuses on a specific aspect of the data, allowing for better error handling, performance, and data integrity.

## Command Structure

The import process uses two top-level commands for processing sales data:

1. Process Invoices:
   ```bash
   python3 -m importer.cli process-invoices <file> [--output results.json] [--batch-size 100] [--error-limit 1000] [--debug]
   ```
   Processes invoice data in five phases:
   1. Company Processing
      - Creates/updates companies from domains
      - Ensures required companies exist
   2. Customer Processing
      - Creates/updates customers with company links
      - Handles customer identification
   3. Product Processing
      - Creates/updates products from line items
      - Handles special items (shipping, tax, etc.)
   4. Invoice Processing
      - Creates/updates invoice headers
      - Links to customers and addresses
   5. Line Item Processing
      - Creates/updates line items
      - Links to invoices and products

2. Process Sales Receipts:
   ```bash
   python3 -m importer.cli process-receipts <file> [--output results.json] [--batch-size 50] [--error-limit 1000] [--debug]
   ```
   Processes receipt data in five phases:
   1. Company Processing
      - Creates/updates companies from domains
      - Ensures required companies exist
   2. Customer Processing
      - Creates/updates customers with company links
      - Handles Amazon FBA special cases
   3. Product Processing
      - Creates/updates products from line items
      - Handles special items (shipping, tax, etc.)
   4. Receipt Processing
      - Creates/updates receipt headers
      - Links to customers and addresses
   5. Line Item Processing
      - Creates/updates line items
      - Links to receipts and products

3. Verification:
   ```bash
   python3 -m importer.cli verify sales <file> [--output results.json]
   ```
   - Validates all relationships
   - Verifies calculations
   - Checks data consistency
   - Reports detailed statistics

## Data Processing Rules

### Product Processing
- Product codes standardized to uppercase
- Special items mapped to system codes:
  * SYS-SHIPPING: All shipping and handling charges (FedEx, UPS, etc.)
  * SYS-TAX: Sales tax
  * SYS-NJ-TAX: New Jersey sales tax
  * SYS-DISCOUNT: Discounts
- Descriptions preserved and updated
- Creation/modification timestamps maintained
- Shipping carrier details stored in line item descriptions

### Line Item Processing
- Validates product existence
- Validates quantities (must be positive)
- Validates unit prices
- Calculates line item amounts
- Processes service dates (optional)
- Special item handling:
  * Shipping: All shipping/handling charges use SYS-SHIPPING product code
    - Original shipping method (e.g., "Fed Ex Ground", "UPS Collect") preserved in description
    - Handles collect shipments with zero amounts
    - Shipping costs may be included in total even if amount is 0
  * Tax: Uses appropriate tax product code based on jurisdiction
  * Discounts: Allows negative amounts for discount items

### Order Processing
- Validates customer existence
- Links to customer addresses
- Maps order status:
  * CLOSED: For paid orders/sales receipts
  * OPEN: For unpaid/partially paid orders
- Processes order metadata:
  * Order date (required)
  * Due date (optional)
  * PO number (optional)
  * Shipping method (optional)
  * Class (optional)

### Payment Processing
- Maps payment status:
  * PAID: For paid orders/sales receipts
  * UNPAID: For open/partially paid orders
- Processes payment terms
- Validates payment dates
- Reconciles payment amounts
- Updates order totals

### Verification Process
1. Reference Validation:
   - Customer references
   - Product references
   - Address references

2. Line Item Validation:
   - Quantity/price validation
   - Amount calculations
   - Service date formats

3. Order Validation:
   - Total calculations
   - Tax amount verification
   - Status consistency

4. Payment Validation:
   - Status consistency
   - Amount reconciliation
   - Terms validation

5. Orphan Detection:
   - Orphaned line items
   - Orphaned orders
   - Disconnected records

## Error Categories

1. **Critical** (Stops Processing)
   - Invalid transaction numbers
   - Missing required relationships
   - Product not found
   - Customer not found
   - Invalid calculations

2. **Warning** (Continues Processing)
   - Non-standard product codes
   - Unusual quantities or amounts
   - Duplicate transactions
   - Missing optional fields
   - Invalid dates

3. **Info** (Statistical)
   - Processing metrics
   - Performance data
   - Record counts
   - Success rates

## Performance Considerations

### Batch Processing
- Each command optimized for large datasets
- Memory-efficient processing
- Batch database operations
- Progress tracking
- Summary statistics

### Transaction Management
- Each phase uses independent transactions
- Automatic rollback on errors
- Session-based operations
- Proper error handling

### Caching Strategy
- Product cache for lookups
- Customer cache for validation
- Address cache for linking
- Memory-aware caching

## Success Criteria

An import is considered successful when:
1. All phases complete without critical errors
2. All relationships are properly established
3. Calculations are accurate and verified
4. No orphaned records exist
5. Data integrity is maintained

## Best Practices

1. Process Order:
   ```bash
   # For invoices:
   python3 -m importer.cli process-invoices input.csv --output invoice-results.json --debug
   
   # For sales receipts:
   python3 -m importer.cli process-receipts input.csv --output receipt-results.json --debug
   
   # Verify results:
   python3 -m importer.cli verify sales input.csv --output verify-results.json
   ```

2. Error Handling:
   - Review verification results
   - Address critical errors first
   - Check warning patterns
   - Validate calculations
   - Ensure data consistency

3. Performance Tips:
   - Process in recommended order
   - Use appropriate batch sizes
   - Monitor memory usage
   - Check verification results
   - Save output for analysis

# Sales Data Import Plan

## Overview
This document outlines the plan for processing sales data from multiple sources (Invoices and Sales Receipts) into our normalized database structure. The process must handle complex multi-line transactions, product variations, pricing rules, and multiple sales channels.

The system must handle two distinct import scenarios:
1. Initial bulk import (~50,000 rows)
2. Incremental updates (~100 rows per import)

## Key Challenges

### Data Structure Complexity
- Multi-line transactions (products, shipping)
- Group related line items together
- Special line item handling (tax, shipping, handling fees, discounts)
- Address line concatenation (combining multiple lines into standard format)
- Maintain transaction integrity

### Product Management
- Product code extraction
- Product descriptions
- Basic product information
- Flat product list

### Order Processing
- Order header creation with status mapping (Paid -> CLOSED/PAID, other -> OPEN/UNPAID)
- Line item processing (excluding special items like tax and shipping)
- Totals calculation including tax percent
- Existing order detection (via QuickBooks ID or order number)
- Source data preservation for audit trail

### Data Quality
- Required field validation
- Amount reconciliation
- Product code validation
- Customer relationships

## Database Schema
We'll use the existing schema from schema.sql which includes:

- Product
  * id (TEXT PRIMARY KEY)
  * productCode (TEXT UNIQUE NOT NULL)
  * name (TEXT NOT NULL)
  * description (TEXT)
  * createdAt/modifiedAt timestamps

- Order
  * id (TEXT PRIMARY KEY)
  * orderNumber (TEXT UNIQUE NOT NULL)
  * customerId (TEXT REFERENCES Customer)
  * orderDate (TIMESTAMP NOT NULL)
  * status (order_status NOT NULL)
  * paymentStatus (payment_status NOT NULL)
  * subtotal (NUMERIC NOT NULL)
  * taxAmount (NUMERIC)
  * totalAmount (NUMERIC NOT NULL)
  * billingAddressId (TEXT REFERENCES Address)
  * shippingAddressId (TEXT REFERENCES Address)
  * paymentMethod (TEXT)
  * terms (TEXT)
  * poNumber (TEXT)
  * class (TEXT)
  * shippingMethod (TEXT)
  * shipDate (TIMESTAMP)
  * quickbooksId (TEXT)
  * sourceData (JSONB)
  * createdAt/modifiedAt timestamps

- OrderItem
  * id (TEXT PRIMARY KEY)
  * orderId (TEXT REFERENCES Order)
  * productCode (TEXT REFERENCES Product)
  * description (TEXT)
  * quantity (NUMERIC NOT NULL)
  * unitPrice (NUMERIC NOT NULL)
  * amount (NUMERIC NOT NULL)
  * serviceDate (TIMESTAMP)
  * sourceData (JSONB)

## Implementation Phases

### Phase 0: CLI Restructuring ✓
**Goal**: Establish robust command infrastructure
- [x] Move from single cli.py to organized modules
- [x] Set up new command structure
- [x] Implement logging and configuration

**Implementation Complete**:

Final directory structure:
```
importer/
├── cli/
│   ├── __init__.py     # Core exports
│   ├── __main__.py     # CLI entry point
│   ├── base.py         # Base command classes
│   ├── config.py       # Configuration handling
│   └── logging.py      # Logging setup
├── commands/
│   ├── __init__.py
│   ├── validate/
│   │   ├── __init__.py # Validation commands
│   └── utils/
│       ├── __init__.py # Utility commands
```

Completed Features:
1. Base Command Infrastructure
   - Implemented BaseCommand with shared functionality
   - Added FileInputCommand and DirectoryInputCommand
   - Created standardized error handling
   - Established database connection management

2. Configuration System
   - Environment variable support
   - Validation of settings
   - Default values handling
   - Runtime configuration options

3. Logging System
   - File and console output
   - Colored formatting for console
   - Configurable log levels
   - Timestamp-based log files

4. Command Structure
   - Modular command organization
   - Consistent command execution patterns
   - Proper error handling and reporting
   - Working command groups:
     * Root: test-connection, validate
     * customers: list-companies, extract-domains, process-addresses,
       process, process-emails, process-phones, verify

All success criteria met:
1. ✓ Existing commands working as expected
2. ✓ Code properly organized in new structure
3. ✓ Logging consistent across commands
4. ✓ Configuration centralized
5. ✓ Error handling standardized

Commands can be run with:
```bash
python3 -m importer.cli test-connection
python3 -m importer.cli validate <file>
```

### Phase 1: Data Validation
**Goal**: Ensure input data quality and structure
- [ ] Implement validation command structure
- [ ] CSV structure validation
- [ ] Required field checks
- [ ] Data quality validation

**Implementation Notes**:
1. Required Fields:
   - Transaction number (Invoice/Receipt)
   - Transaction date
   - Customer information
   - Line items
   - Amounts/totals

2. Data Quality:
   - Date formats and ranges
   - Amount reconciliation
   - Tax calculations
   - Customer data completeness
   - Product code validation
   - Special items (tax, shipping, etc.)
   - Address formatting

3. Command: `python3 -m importer.cli validate-sales <file>`

### Phase 2: Product Processing
**Goal**: Create and update product records
- [ ] Product extraction and validation
- [ ] Product record creation/updates
- [ ] Product code management

**Implementation Notes**:
1. Product Handling:
   - Extract from line items
   - Handle duplicates
   - Maintain uniqueness
   - Store descriptions

2. Command: `python3 -m importer.cli process products <file>`

### Phase 3: Order Processing
**Goal**: Create order records with line items
- [ ] Order header creation
- [ ] Line item processing
- [ ] Special item handling
- [ ] Status and payment tracking

**Implementation Notes**:
1. Processing Features:
   - Group by invoice/receipt
   - Calculate totals and tax
   - Filter special items
   - Handle existing orders
   - Batch processing
   - Source data preservation

2. Command: `python3 -m importer.cli process orders <file>`

### Phase 4: Verification
**Goal**: Verify data integrity
- [ ] Reference checking
- [ ] Total reconciliation
- [ ] Relationship validation

**Implementation Notes**:
1. Verification Tasks:
   - Product references
   - Order totals
   - Customer relationships
   - No orphaned records

2. Command: `python3 -m importer.cli verify sales <file>`

## Progress Tracking

Each phase will track:
1. Records processed
2. Success/failure counts
3. Validation issues
4. Processing statistics

## Completion Criteria

The import is considered successful when:
1. All phases complete without critical errors
2. Order totals reconcile
3. All relationships verified
4. No orphaned records exist

## Error Handling

Errors will be categorized as:
1. **Critical** - Stops processing
   - Invalid transaction numbers
   - Missing required relationships
   - Product not found
   
2. **Warning** - Logged but continues
   - Non-standard product codes
   - Unusual quantities
   
3. **Info** - Tracked for reporting
   - Processing statistics
   - Performance metrics

## Performance Considerations

1. **Bulk Import Mode**
   - Handles ~50,000 rows of initial data
   - Batch processing for efficiency
   - Progress tracking essential
   - Memory management for large datasets
   - Summarized logging output

2. **Incremental Update Mode**
   - Handles ~100 rows per import
   - Real-time processing acceptable
   - Immediate feedback on changes
   - Focus on accuracy over speed
   - Detailed interactive output

## Reporting Modes

1. **Bulk Processing Mode**
   - Minimal console output
   - Summary statistics only
   - Progress indicators (% complete)
   - Error counts by category
   - Output logged to file for review
   - Focus on performance

2. **Interactive Mode**
   - Verbose console output
   - Real-time status updates
   - Detailed error messages
   - Record-by-record feedback
   - Direct user feedback
   - Focus on visibility

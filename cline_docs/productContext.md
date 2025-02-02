# Product Context

## Purpose
This system imports sales data from QuickBooks into a structured database, enabling:
- Data analysis and reporting
- Integration with other systems
- Historical tracking of sales
- Customer relationship management

## Problems Solved

### Data Integration
- Imports QuickBooks sales receipts and invoices
- Maintains relationships between customers, orders, and products
- Preserves QuickBooks IDs for data consistency
- Handles special cases like Amazon FBA

### Customer Management
1. Customer Identification:
   - Uses QuickBooks ID as primary identifier when available
   - Falls back to name matching with normalization
   - Handles special case for Amazon FBA with city-specific names
   - Generates UUIDs for customers without QuickBooks IDs

2. Customer Data Quality:
   - Prevents duplicate customer records
   - Updates customer information when it changes
   - Maintains consistent naming for Amazon FBA locations
   - Tracks customer domains for communication

3. Idempotent Processing:
   - Safe to run multiple times
   - Updates existing records instead of creating duplicates
   - Preserves relationships across imports
   - Handles data changes gracefully

### Error Handling
- Validates data before processing
- Tracks errors for debugging
- Continues processing on recoverable errors
- Stops on critical errors
- Provides detailed error reporting

## How It Works

### Data Flow
1. Read sales data from CSV files
2. Process customers first to establish relationships
3. Process sales receipts/invoices
4. Process line items and product mappings
5. Generate validation reports

### Key Features
- Batch processing for performance
- Error tracking and reporting
- Data validation
- Idempotent processing
- Special case handling
- Relationship maintenance

### Integration Points
- QuickBooks data export (input)
- Database storage (output)
- Error reporting system
- Validation system

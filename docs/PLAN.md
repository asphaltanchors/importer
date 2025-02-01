# CSV Import Implementation Plan

## Project Structure
See STRUCTURE.md for the base project layout. Additional directories to be added:

```
/
├── samples/           # Test data directory (gitignored)
    └── *.csv         # CSV files following naming convention:
                      # {Type}_{MM}_{DD}_{YYYY}_{H}_{MM}_{SS}.csv
                      # Example: Customer_01_31_2025_1_00_02.csv
```

## File Naming Convention
- Files are named in the format: Type_MM_DD_YYYY_H_MM_SS.csv
- Three types of files:
  1. Customer files (generated daily at 1:00:02 AM)
  2. Invoice files (generated daily at 2:00:02 AM)
  3. Sales Receipt files (generated daily at 3:00:02 AM)

## Implementation Phases

### Phase 1: Project Setup
1. Create directory structure
2. Set up .gitignore for samples directory
3. Configure database connection handling
4. Implement base CSV parsing utilities

### Phase 2: Customer Import
1. Create customer data model/schema
2. Implement customer CSV parser
3. Add validation rules for customer data
4. TODO: Define company inference logic and implementation
5. Create sample customer data files
6. Implement tests for customer import

### Phase 3: Invoice Import
1. Create invoice data model/schema
2. Implement invoice CSV parser
3. Add validation rules for invoice data
4. Handle multi-line invoice entries
5. Create sample invoice data files
6. Implement tests for invoice import

### Phase 4: Sales Receipt Import
1. TODO: Define sales receipt handling approach
2. Create sales receipt data model/schema
3. Implement sales receipt CSV parser
4. Handle multi-line sales entries
5. Create sample sales receipt data files
6. Implement tests for sales receipt import

## Import Sequence Rules
1. Customers must be imported first
   - Required for invoice and sales receipt foreign key relationships
   - Establishes company relationships
2. Invoices and sales receipts can be imported in any order after customers
   - Both depend on customer records being present
   - No interdependencies between invoices and sales receipts

## Data Validation Rules
1. Customer Import
   - Required fields validation
   - Address format validation
   - Email format validation
   - Phone number format validation
   - Duplicate detection

2. Invoice Import
   - Customer existence validation
   - Required fields validation
   - Date format validation
   - Numeric field validation
   - Line item validation

3. Sales Receipt Import
   - Customer existence validation
   - Required fields validation
   - Date format validation
   - Numeric field validation
   - Line item validation

## Error Handling
1. Validation Errors
   - Field-level validation errors
   - Record-level validation errors
   - Relationship validation errors

2. Processing Errors
   - CSV parsing errors
   - Database errors
   - System errors

3. Error Reporting
   - Error logging
   - Error summary reports
   - Detailed error messages for debugging

## Testing Strategy
1. Unit Tests
   - Individual component testing
   - Validation rule testing
   - Error handling testing

2. Integration Tests
   - End-to-end import testing
   - Database integration testing
   - Multi-file import testing

3. Test Data
   - Sample files for happy path testing
   - Sample files with various error conditions
   - Large dataset performance testing

## Open Questions/TODOs
1. Company Inference
   - Define fields used for company inference
   - Determine matching/creation rules
   - Handle conflicts/duplicates

2. Sales Receipt Implementation
   - Define specific requirements
   - Determine table structure/relationships
   - Implement specific validation rules

## Next Steps
1. Begin with project structure setup
2. Create sample data directory
3. Implement customer import functionality
4. Add tests for customer import
5. Review and refine company inference approach
6. Proceed with invoice implementation
7. Define and implement sales receipt handling

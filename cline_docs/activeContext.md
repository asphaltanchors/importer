# Active Context

## Current Focus
Standardizing test patterns across processor test files.

## Recent Changes
- Analyzed test_company_processing.py for established patterns
- Identified need for consistent test structure
- Found gaps in current test implementations
- Discovered successful patterns to replicate
- Mapped out comprehensive test strategy

## Next Steps

### Test Pattern Standardization
1. Core Functionality Tests:
   - Basic creation/processing (happy path)
   - Name/domain normalization
   - Idempotent processing
   - Batch processing
   - Relationship handling

2. Error Handling Tests:
   - Invalid data scenarios
   - Error limits
   - Partial success cases
   - Validation rules
   - Warning vs critical errors

3. Relationship Tests:
   - Entity relationships
   - Lookup patterns
   - Integrity constraints
   - Update behaviors

### Implementation Order
1. test_customer_import.py:
   - Rewrite basic functionality tests
   - Add normalization tests
   - Add idempotency tests
   - Add batch processing tests
   - Add error handling
   - Add validation tests
   - Add relationship tests

2. Other Processor Tests:
   - Apply same patterns to test_invoice_import.py
   - Update test_product_processing.py
   - Review and align other test files

## Active Decisions
- Follow established patterns from test_company_processing.py
- Each test should be focused and self-contained
- Clear test names describing behavior
- Consistent structure across all processor tests
- Strong validation of results
- Proper error handling checks

## Implementation Notes
- Use session_manager consistently
- Follow processor initialization patterns
- Create focused test data
- Verify through stats
- Maintain test isolation
- Use debug logging for clarity

## Future Work

### Customer Import Features
- Support for normalized name matching
- Handle comma-separated names
- Support percentage notation in names
- Handle business suffix variations
- Maintain QuickBooks ID relationships

### Customer Data Validation
- Required fields validation
- Format validation for names and IDs
- Domain relationship validation
- Address relationship validation

### Error Handling
- Handle missing company domains
- Handle invalid billing addresses
- Handle invalid shipping addresses
- Track validation errors
- Support batch error limits

### Performance Optimizations
- Company domain caching
- Address ID caching
- Batch size tuning
- Session management
- Transaction handling

### Testing Infrastructure
- Test isolation patterns
- Transaction management
- Session handling
- Data cleanup
- Fixture organization

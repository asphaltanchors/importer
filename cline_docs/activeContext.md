# Active Context

## Current Focus
Fixing test patterns in customer import tests to align with system patterns and existing test practices.

## Recent Changes
- Identified misalignment between customer import tests and established patterns
- Discovered correct test pattern in test_company_processing.py
- Found issues with transaction management and test isolation

## Next Steps

### Test Pattern Alignment
1. Rewrite test_customer_import.py to follow established patterns:
   - Use session_manager instead of populated_session
   - Remove manual entity creation
   - Process through proper interfaces

2. Test Structure Changes:
   - Create test data with both company and customer info
   - Process with CompanyProcessor first
   - Then process with CustomerProcessor
   - Assert results through stats and queries

3. Test Cases to Implement:
   - test_customer_creation_basic: Basic flow with company creation first
   - test_customer_domain_normalization: Handle domain variations
   - test_customer_idempotent_processing: Verify idempotency
   - test_customer_batch_processing: Test batch handling
   - test_customer_error_handling: Validate error cases

### Key Patterns to Follow
1. Data Processing Sequence:
   - Companies must be processed before customers
   - Use proper processor interfaces
   - No manual entity creation

2. Test Isolation:
   - Each test is self-contained
   - Use session_manager for clean state
   - Process all data through processors

3. Test Focus:
   - Each test verifies specific behavior
   - Clear test names describing behavior
   - Assertions focus on processor stats and results

## Active Decisions
- Moving away from populated_session in processor tests
- Following established patterns from test_company_processing.py
- Maintaining proper processing sequence in tests

## Implementation Notes
- Use DataFrame for test data creation
- Process through proper interfaces
- Assert through processor stats
- Maintain test isolation
- Follow naming conventions from company tests

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

# Active Context

## Current Task
Standardizing sales processing workflow across invoices and receipts:
- Both flows need consistent phases: company -> customer -> product -> order -> line item
- Invoice processing missing company phase
- Product processing needs to be reintegrated
- Documentation needs updating

## Action Plan

### Phase 1: Invoice Company Processing
1. Add company processing to invoice flow
   - Add CompanyProcessor to ProcessInvoicesCommand
   - Convert Config to dictionary format (matching receipt flow)
   - Update logging to match receipt's phase structure
   - Test company creation/lookup

### Phase 2: Product Processing
1. Review and update ProductProcessor
   - Add critical_issues/warnings validation (matching receipt flow)
   - Add robust error tracking with ErrorTracker
   - Add validation rules for product data
   - Test product creation/lookup

2. Integrate ProductProcessor
   - Add as Phase 3 in ProcessInvoicesCommand
   - Add as Phase 3 in ProcessReceiptsCommand (between customer and receipt)
   - Update logging in both commands
   - Test product processing works in both flows

### Phase 3: Documentation Updates
1. Update system documentation
   - Update SALES.md with new processing sequence
   - Update STRUCTURE.md if needed
   - Review and update any other relevant docs

2. Update Memory Bank
   - Update productContext.md with current purpose/problems
   - Update systemPatterns.md with new architecture
   - Update techContext.md if needed
   - Update progress.md with completed changes

### Phase 4: Testing & Verification
1. Test full processing sequence
   - Verify company->customer->product->order->line item flow
   - Check logging shows clear phase progression
   - Verify error handling across all phases

2. Create test cases
   - Add tests for company processing in invoice flow
   - Add tests for product processing in both flows
   - Update existing tests to match new sequence

## Future Improvements
After completing the current standardization work, several architectural improvements have been identified:

### Code Quality Improvements
1. Processor Base Class Improvements:
   - Add abstract base class (ABC) to enforce implementation
   - Add type hints throughout
   - Add dataclass for stats tracking
   - Add context manager support

2. Error Handling:
   - Standardize error tracking across processors
   - Add custom exceptions for different error types
   - Move error tracking to base class

3. Validation:
   - Standardize validation across processors
   - Move common validation to base class
   - Make validate_data() abstract

4. Performance:
   - Add timing decorators in base class
   - Standardize progress reporting
   - Add consistent performance metrics

5. Configuration:
   - Move field mappings to config files
   - Add processor configuration dataclass
   - Add config validation

6. Processing Pipeline:
   - Add ProcessingPipeline class
   - Make processors composable
   - Add dependency injection
   - Add pre/post processing hooks

## Recent Changes
1. Modified ProcessReceiptsCommand to:
   - Add Phase 1 for company processing using CompanyProcessor
   - Move customer processing to Phase 2
   - Move receipt processing to Phase 3
   - Move line item processing to Phase 4

2. Added company creation to ProcessReceiptCustomersCommand:
   - Added _ensure_required_companies method
   - Creates amazon-fba.com and unknown-domain.com companies if needed
   - Ensures companies exist before customer creation

3. Fixed configuration handling:
   - Modified CompanyProcessor initialization to work with Config object
   - Convert Config object to dictionary with required fields

## Next Steps
1. Begin with Phase 1: Adding company processing to invoice flow
   - First implement Config to dictionary conversion
   - Then add CompanyProcessor integration
   - Finally update logging to match phase structure
2. Each change should be small and testable
3. Update this plan as we progress to keep it accurate

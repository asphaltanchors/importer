# Sales Import Refactor Plan

This document outlines the step-by-step approach to refactoring the sales import process to use a multi-pass approach similar to the customer import process.

## Phase 1: Move Product Processing to Sales Command

### Step 1: Move Product Command
1. Create new `ProcessProductsCommand` in `importer/commands/sales/products.py`
   - Copy existing product processing logic from `importer/commands/products/__init__.py`
   - Update imports to reflect new location
   - No changes to processing logic needed

2. Update CLI Structure in `importer/cli/__main__.py`
   - Add `process-products` subcommand to sales group
   - Remove products command group
   - Update imports to point to new location

3. Update Documentation
   - Move product processing documentation to SALES.md
   - Update CLI usage examples
   - Add deprecation notice for old products command

### Step 2: Clean Up Old Product Code
1. Remove old product command files:
   - `importer/commands/products/__init__.py`
   - `importer/commands/products/` directory
2. Update any import statements in other files
3. Remove products group from CLI help text

## Phase 2: Implement Line Item Processing

### Step 1: Create Line Item Command
1. Create `ProcessLineItemsCommand` in `importer/commands/sales/line_items.py`
   - Extract line item logic from current invoice processor
   - Create dedicated processor for line items
   - Add caching for processed items

2. Update CLI Structure
   - Add `process-line-items` subcommand to sales group
   - Update help text and documentation

3. Implement Features
   - Product linkage validation
   - Quantity and pricing processing
   - Amount calculations
   - Service date handling
   - Cache results for order processing

## Phase 3: Implement Order Processing

### Step 1: Create Order Command
1. Create `ProcessOrdersCommand` in `importer/commands/sales/orders.py`
   - Extract order header logic from current invoice processor
   - Create dedicated order processor
   - Link to cached line items

2. Update CLI Structure
   - Add `process-orders` subcommand to sales group
   - Update help text and documentation

3. Implement Features
   - Customer validation
   - Status mapping
   - Payment terms processing
   - Date handling
   - Link to processed line items

## Phase 4: Implement Payment Processing

### Step 1: Create Payment Command
1. Create `ProcessPaymentsCommand` in `importer/commands/sales/payments.py`
   - Extract payment logic from current invoice processor
   - Create dedicated payment processor

2. Update CLI Structure
   - Add `process-payments` subcommand to sales group
   - Update help text and documentation

3. Implement Features
   - Payment status processing
   - Payment terms handling
   - Payment date validation
   - Amount reconciliation
   - Link to processed orders

## Phase 5: Update Verification Process

### Step 1: Enhance Verify Command
1. Update `VerifyCommand` in `importer/commands/verify/__init__.py`
   - Add verification for line items
   - Add payment verification
   - Enhance relationship checking

2. Implement New Verification Features
   - Line item validation
   - Payment reconciliation
   - Enhanced total calculations
   - Relationship verification
   - Orphaned record detection

## Phase 6: Clean Up and Documentation

### Step 1: Code Cleanup
1. Remove deprecated code
2. Update import statements
3. Ensure consistent error handling
4. Add logging improvements

### Step 2: Documentation Updates
1. Update SALES.md with new command structure
2. Add examples for each command
3. Update error handling documentation
4. Add performance recommendations

### Step 3: Testing
1. Update existing tests
2. Add new test cases for:
   - Line item processing
   - Payment handling
   - Multi-pass verification
   - Error scenarios

## Implementation Approach

Each phase should be implemented and tested independently:

1. Create new command/processor
2. Update CLI structure
3. Implement features
4. Add tests
5. Update documentation
6. Review and merge

This allows for incremental improvements while maintaining a working system throughout the refactor.

## Testing Strategy

For each phase:
1. Unit tests for new processors
2. Integration tests for command flow
3. End-to-end tests for full process
4. Performance testing with large datasets

## Success Criteria

The refactor is complete when:
1. All new commands are implemented and tested
2. Old product command is fully removed
3. Documentation is updated
4. Tests pass and cover new functionality
5. Performance matches or exceeds current implementation

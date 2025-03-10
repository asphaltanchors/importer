# System Patterns

## CLI Organization

### Command Structure
1. Top-Level Commands:
   - process-invoices: Primary command for invoice data import
   - process-receipts: Primary command for sales receipt import
   - test-connection: Database connectivity test
   - validate: General validation commands

2. Subcommand Groups:
   - customers: Customer data management operations
   - sales: Specialized sales operations
   - verify: Data verification tools

### Command Patterns
- Top-level commands for primary operations
- Subcommands for domain-specific operations
- Consistent parameter patterns (--output, --batch-size, --error-limit)
- Debug mode available for all commands

## Data Processing Phases
The system processes data in a standardized sequence across both invoice and receipt flows:

1. Company Processing (first to establish base relationships)
   - Creates/updates companies from customer domains
   - Ensures required companies exist (amazon-fba.com, unknown-domain.com)
   - Must run first as customers depend on companies
   - Uses CompanyProcessor with error tracking
   - Validates domain formats and relationships

2. Customer Processing (uses company relationships)
   - Creates/updates customers with company relationships
   - Handles special cases like Amazon FBA
   - Must run after companies but before orders
   - Uses customer-specific processors for each flow
   - Maintains QuickBooks ID relationships

3. Product Processing (independent phase)
   - Creates/updates products from line items
   - Handles special cases (shipping, tax, discounts)
   - Maps raw product codes to system codes
   - Uses ProductProcessor with ErrorTracker
   - Validates product codes and descriptions
   - Enforces business rules (e.g., no test products)
   - Must run before line items to ensure products exist

4. Order Processing (uses customer relationships)
   - Creates/updates orders (invoices/receipts)
   - Links to customers and addresses
   - Must run after customers but before line items
   - Uses flow-specific processors (InvoiceProcessor/SalesReceiptProcessor)
   - Validates order data and relationships

5. Line Item Processing (uses order and product relationships)
   - Creates/updates line items
   - Links to orders and products
   - Must run last as it depends on both orders and products
   - Uses flow-specific processors
   - Validates product relationships and quantities

## Idempotency
The system is designed to be idempotent - running the same import multiple times produces the same result:

1. Primary Key Strategy:
   - Use external IDs (like QuickBooks ID) as primary keys when available
   - Fall back to generated UUIDs only when external IDs aren't available
   - This maintains stable relationships across imports

2. Lookup Order:
   - First try external ID (most reliable)
   - Then try exact name/domain match
   - Then try normalized name/domain match
   - Only create new if no match found

3. Update vs Create:
   - Update existing records instead of creating duplicates
   - Track what's been processed to avoid duplicates within a run
   - Update fields that might change (like names)
   - Preserve fields that shouldn't change (like creation date)

4. Special Cases:
   - Required companies (amazon-fba.com, unknown-domain.com)
   - Amazon FBA uses city-specific names
   - Normalized customer names for better matching
   - Domain handling (amazon-fba.com, email domain, unknown-domain.com)
   - System products (shipping, tax, discounts)

## Error Handling
- Batch processing with error limits
- Continue on recoverable errors
- Stop on critical errors
- Track error context for debugging
- Validate data before processing
- Each phase has its own validation rules

## Testing Patterns

### Database Test Isolation
- Use autouse fixture to truncate tables between tests
- Ensure clean state for each test run
- Prevent test interdependencies
- Make test failures deterministic
- Follow database testing best practices

### Test Fixtures
- session_manager: Creates database session managers
- session: Provides transactional test sessions
- populated_session: Sets up common test data
- clean_tables: Ensures test isolation
- Fixtures follow clear dependency chain

## Database Operations

### Session Management
- Use SessionManager class for all database operations
- Initialize processors with config dictionary containing database_url
- Use context manager pattern for automatic session cleanup
- Commit in batches for performance
- Use with_for_update() for row locking
- Cascade deletes for related records
- Maintain referential integrity across phases

### Processor Architecture
1. Base Class Pattern:
   - BaseProcessor[T] abstract base class
   - Type-safe configuration with Generic[T]
   - Abstract validate_data() method
   - Abstract _process_batch() method
   - Common batch processing logic
   - Standardized error handling

2. Stats Tracking:
   - Dynamic ProcessingStats class
   - Support both attribute and dictionary access
   - Automatic handling of processor-specific stats
   - Built-in timing metrics
   - JSON-serializable output

3. Error Handling:
   - ErrorTracker integration
   - Batch-level error recovery
   - Error limit enforcement
   - Detailed error context
   - Debug logging support

4. Validation:
   - Two-phase validation (critical/warnings)
   - Pre-processing data validation
   - Row-level validation during processing
   - Clear validation messages
   - Validation summary logging

## Logging
- Debug mode for detailed logs
- Track statistics for monitoring
- Log validation issues
- Track processing time
- Phase-specific logging with visual separation
- Clear phase progression in logs
- Standardized format across all phases:
  * Phase headers with visual separation
  * Validation summaries with item counts
  * Consistent logging levels (info/debug)
  * Progress reporting with batch counts
  * Error summaries with context

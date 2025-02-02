# System Patterns

## Data Processing Phases
The system processes data in distinct phases to maintain separation of concerns:
1. Company Processing (first to establish base relationships)
2. Customer Processing (uses company relationships)
3. Receipt/Invoice Processing (uses customer relationships)
4. Line Item Processing (uses order relationships)

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

## Error Handling
- Batch processing with error limits
- Continue on recoverable errors
- Stop on critical errors
- Track error context for debugging
- Validate data before processing

## Database Operations
- Use session management for transactions
- Commit in batches for performance
- Use with_for_update() for row locking
- Cascade deletes for related records

## Logging
- Debug mode for detailed logs
- Track statistics for monitoring
- Log validation issues
- Track processing time

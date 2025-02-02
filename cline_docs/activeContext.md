# Active Context

## Current Task
Improving customer handling in sales receipt processing:
- Added a dedicated customer processing phase before receipt processing
- Made customer processing idempotent
- Special handling for Amazon FBA customers with city-specific names

## Recent Changes
1. Created new ProcessReceiptCustomersCommand that:
   - Handles customer creation/matching logic
   - Includes special handling for Amazon FBA cases
   - Creates customers with appropriate domains (amazon-fba.com, email domain, or unknown-domain.com)

2. Modified ProcessReceiptsCommand to:
   - Add Phase 1 for customer processing
   - Move receipt processing to Phase 2
   - Move line item processing to Phase 3

3. Made customer processing idempotent:
   - Use QuickBooks ID as primary key when available
   - Generate UUID only for customers without QuickBooks ID
   - Proper lookup order: QuickBooks ID -> Name -> Create New
   - Update existing customers instead of creating duplicates
   - Track processed QuickBooks IDs to avoid duplicates within a run

## Next Steps
1. Consider adding validation for QuickBooks IDs to ensure they follow expected format
2. Add logging for customer updates to track changes over time
3. Consider adding a way to merge duplicate customers if found

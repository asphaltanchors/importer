# Customer Data Import Plan

## Overview
This document outlines the plan for processing customer data from CSV files into our normalized database structure. The process will handle customer information, addresses, contact details, and company relationships.

## Requirements

### Performance
- Process files efficiently to minimize import time
- Handle batch operations to reduce database load
- Support processing of large files without memory issues

### Data Quality
- Validate required fields before processing
- Log issues for review without stopping the process
- Track success/failure statistics

### Data Handling Rules
- Multiple emails supported (split on semicolons/commas)
- First valid email domain determines company association
- Phone numbers stored in standardized format
- Addresses stored as-is for display purposes
- Identical billing/shipping addresses should be deduplicated

## Processing Phases

### Phase 1: Initial Data Validation ✓
**Goal**: Ensure input data quality before processing
- [x] Validate CSV structure and required fields
- [x] Pre-validate foreign key relationships
- [x] Log validation issues

**Implementation Notes**:
1. Required Fields:
   - Only Customer Name and QuickBooks Internal Id are strictly required
   - All other fields are optional to handle real-world data variations
   - Email addresses tracked but not required (logged as warnings if missing)

2. Address Handling:
   - All address fields are optional
   - Incomplete addresses logged as warnings
   - Identical billing/shipping addresses detected for optimization
   - No strict validation of international addresses
   - US/Canada address formats logged but not enforced

3. Data Quality Approach:
   - Focus on importing as much valid data as possible
   - Use warnings instead of errors for non-critical issues
   - Track validation statistics for monitoring
   - Provide detailed feedback without blocking import

4. Validation CLI:
   - Command: `python3 -m importer.cli validate <file>`
   - Shows summary statistics
   - Color-coded output (red=critical, yellow=warning, blue=info)
   - Optional JSON output file for detailed results

5. Key Learnings:
   - Real customer data often incomplete but still valuable
   - Better to warn about issues than block import
   - Address validation should be lenient (human-readable display)
   - Multiple email formats and locations in input data
   - Identical addresses common (optimization opportunity)

### Phase 2: Email Domain Extraction & Company Creation
**Goal**: Establish company records (required for customer foreign keys)
- [ ] Extract and validate email domains
- [ ] Create/update Company records with domains
- [ ] Verify company existence before proceeding
Note: Company records must exist before customer creation due to foreign key constraint

### Phase 3: Address Processing
**Goal**: Create address records (required for customer foreign keys)
- [ ] Process billing addresses as-is
- [ ] Process shipping addresses as-is
- [ ] Deduplicate identical billing/shipping addresses
- [ ] Basic cleanup (trim whitespace, remove empty lines)
Note: Address records must exist before customer creation due to foreign key constraints

### Phase 4: Customer Record Creation
**Goal**: Create customer records with validated foreign keys
- [ ] Verify company domain exists
- [ ] Verify address IDs exist
- [ ] Create Customer records with:
  - Verified companyDomain
  - Verified billingAddressId
  - Verified shippingAddressId
- [ ] Process business details (terms, tax info)

### Phase 5: Contact Information
**Goal**: Add contact details to existing customers
- [ ] Process email addresses
  - [ ] Verify customer ID exists
  - [ ] Set primary email flags (first email is primary)
  - [ ] Create CustomerEmail records
- [ ] Process phone numbers
  - [ ] Verify customer ID exists
  - [ ] Set primary phone flags (first number is primary)
  - [ ] Create CustomerPhone records
Note: Customer records must exist before creating contact info due to foreign key constraints

### Phase 6: Verification & Cleanup
**Goal**: Ensure data integrity and completeness
- [ ] Verify all relationships are properly set
- [ ] Check for orphaned records
- [ ] Generate import summary report

## Progress Tracking

Each phase will track:
1. Records processed
2. Success/failure counts
3. Validation issues
4. Relationship creation status

## Completion Criteria

The import is considered successful when:
1. All phases complete without critical errors
2. All required relationships are established
3. Data validation passes
4. No orphaned records exist
5. Import summary report shows expected counts

## Error Handling

Errors will be categorized as:
1. **Critical** - Stops processing
2. **Warning** - Logged but continues
3. **Info** - Tracked for reporting

Each error will include:
- Timestamp
- Phase
- Record identifier
- Error description
- Resolution status

## Data Processing Rules

### Email Processing
- Split multiple emails on semicolons/commas
- First email in list is considered primary
- First valid email domain used for company association

### Phone Processing
- Remove formatting characters for storage
- Preserve extensions in format " x123"
- First phone number is considered primary

### Company Processing
- Companies identified by email domain
- Domain extracted from first valid email address
- Company must exist before customer creation

### Duplicate Handling
- QuickBooks Internal Id used as primary duplicate detection
- Similar records treated as distinct if they have different QuickBooks IDs
- Identical billing/shipping addresses should reference same address record

### Status Tracking
- Track import progress
- Record statistics:
  - Companies created/updated
  - Customers created
  - Customers updated
  - Addresses created
  - Records processed
  - Warnings/issues

## Open Questions

1. What is the expected volume of records per import?
2. Should we implement any data quality reporting for manual review?
3. How should we handle records with no email domain?
4. What should happen if company domain extraction fails?

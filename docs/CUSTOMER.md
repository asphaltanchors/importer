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

### Phase 2: Email Domain Extraction & Company Creation ✓
**Goal**: Establish company records (required for customer foreign keys)
- [x] Extract and validate email domains
- [x] Create/update Company records with domains
- [x] Verify company existence before proceeding
Note: Company records must exist before customer creation due to foreign key constraint

**Implementation Notes**:
1. Email Domain Extraction:
   - Successfully extracts domains from multiple email fields
   - Handles various email formats (semicolon/comma separated lists)
   - Searches beyond standard email fields (notes, phone fields)
   - First valid email domain is used for company association
   - Basic domain validation (requires @ and valid TLD)

2. Domain Processing Approach:
   - Process domains before company creation
   - Track unique domains across all records
   - Maintain statistics for monitoring and verification
   - Log rows without valid domains for review

3. Key Learnings:
   - Emails often found in unexpected fields
   - Multiple email formats need normalization
   - Some customers may lack valid email domains
   - Domain extraction should be separate from company creation
   - Statistics tracking essential for verification

4. CLI Commands:
   - `python3 -m importer.cli extract-domains <file>`: Process CSV files and create company records
     * Shows summary statistics
     * Lists unique domains found
     * Optional JSON output for detailed results
     * Validates file before processing
   - `python3 -m importer.cli list-companies --limit N`: View N most recent companies
     * Shows total count and most recent records
     * Helps verify company creation
     * Supports pagination for large datasets

5. Company Creation:
   - Uses SQLAlchemy models for database interaction
   - Generates UUIDs for company IDs
   - Creates simple company names from domains
   - Handles duplicate domains gracefully
   - Maintains created timestamps

### Phase 3: Address Processing ✓
**Goal**: Create address records (required for customer foreign keys)
- [x] Process billing addresses as-is
- [x] Process shipping addresses as-is
- [x] Deduplicate identical billing/shipping addresses
- [x] Basic cleanup (trim whitespace, remove empty lines)
Note: Address records must exist before customer creation due to foreign key constraints

**Implementation Notes**:
1. Database Implementation:
   - Created Address SQLAlchemy model with schema fields:
     * id (TEXT PRIMARY KEY) - Using first 32 chars of content hash
     * line1 (TEXT NOT NULL)
     * line2 (TEXT)
     * line3 (TEXT)
     * city (TEXT NOT NULL)
     * state (TEXT NOT NULL)
     * postalCode (TEXT NOT NULL)
     * country (TEXT NOT NULL)

2. Key Features:
   - Efficient deduplication using content-based hashing
   - In-memory caching of processed addresses
   - Automatic type conversion (e.g., numeric postal codes)
   - Whitespace normalization
   - Handles missing/optional fields
   - Detects identical billing/shipping addresses

3. Processing Approach:
   - Uses pandas DataFrame for efficient batch processing
   - Generates stable, content-based IDs for deduplication
   - Single-pass processing of both billing/shipping addresses
   - Maintains address cache to minimize database queries
   - Commits addresses in batches for performance

4. CLI Support:
   - Command: `python3 -m importer.cli process-addresses <file>`
     * Shows detailed processing statistics
     * Reports unique vs duplicate addresses
     * Tracks billing/shipping address counts
     * Optional JSON output for detailed results

5. Key Learnings:
   - Content-based hashing effective for deduplication
   - High rate of billing/shipping address matches (~80%)
   - Postal codes require special handling (numeric vs string)
   - In-memory caching significantly improves performance
   - Batch commits reduce database load

### Phase 4: Customer Record Creation ✓
**Goal**: Create customer records with validated foreign keys
- [x] Verify company domain exists
- [x] Verify address IDs exist
- [x] Create Customer records with:
  - Verified companyDomain
  - Verified billingAddressId
  - Verified shippingAddressId
- [x] Process business details (terms, tax info)

**Implementation Notes**:
1. Database Implementation:
   - Created Customer SQLAlchemy model with schema fields:
     * id (TEXT PRIMARY KEY)
     * customerName (TEXT NOT NULL)
     * companyDomain (TEXT REFERENCES Company)
     * quickbooksId (TEXT)
     * status (TEXT NOT NULL DEFAULT 'ACTIVE')
     * Various business fields (terms, taxCode, etc.)
     * billingAddressId (TEXT REFERENCES Address)
     * shippingAddressId (TEXT REFERENCES Address)
     * sourceData (JSONB NOT NULL DEFAULT '{}')
     * createdAt/modifiedAt timestamps

2. Key Features:
   - Efficient domain and address validation using in-memory caching
   - Handles missing company domains gracefully
   - Supports optional address relationships
   - Maintains audit timestamps
   - Stores source data in JSON format

3. Processing Approach:
   - Validates foreign keys before creation
   - Extracts domains from email fields if needed
   - Maintains performance with cached lookups
   - Commits customers in batches
   - Tracks detailed statistics

4. CLI Support:
   - Command: `python3 -m importer.cli process-customers <file>`
     * Shows detailed processing statistics
     * Reports created/skipped records
     * Tracks relationship validation
     * Optional JSON output for detailed results

5. Key Learnings:
   - Default values critical for required fields
   - JSON fields need explicit empty defaults
   - Timestamp handling needs both created/modified
   - Foreign key validation essential before inserts
   - Cache foreign key values for performance
   - Some customers may lack company domains

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

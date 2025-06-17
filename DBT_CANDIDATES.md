# DBT Pipeline Candidates

This file tracks opportunities to move logic from the dashboard app into the DBT pipeline for better performance and maintainability.

**Context**: Small ecommerce site (~2,000 orders/year, max 10 line items/order = ~20,000 total records)
**Philosophy**: Balance complexity vs. optimization - real-time aggregation is fine for this scale, focus DBT efforts on data quality and business logic rather than performance optimization.

## High Priority (Data Quality & Business Logic)

### ⚠️ Critical: Date Fields Returning as Strings *(MOVED UP)*
**Current State**: Date fields defined as `date` but return strings in queries
**Impact**: Cannot use native Date operations, requires workarounds
**DBT Fix**: Fix staging layer to ensure proper date typing
**Rationale**: This affects all date-based analytics and creates technical debt

### Order Status Standardization *(MOVED UP)*
**Current State**: Free-text status values requiring complex UI logic
**DBT Candidate**: Create standardized status enum/lookup
**Benefits**: Cleaner UI code, reliable status-based analytics
**Complexity**: Low - simple CASE WHEN mapping
**Rationale**: Business logic belongs in pipeline, not application

## Medium Priority (Business Logic Worth Centralizing)

### Sales Price & Purchase Cost Typing
**Current State**: Stored as TEXT, preventing margin calculations
**DBT Candidate**: Cast to NUMERIC in staging, handle invalid values
**Benefits**: Enables profit margin analytics without app-level casting
**Complexity**: Low - simple type casting with error handling
**Rationale**: Data typing belongs in pipeline, enables business calculations

### Customer Lifetime Value (CLV)
**Current State**: Not yet implemented
**DBT Candidate**: Create `dim_customer_metrics` with CLV, frequency, AOV
**Benefits**: Pre-calculated customer insights
**Complexity**: Medium - customer cohort analysis
**Rationale**: Complex calculation worth centralizing, but not urgent at this scale

## Low Priority (Probably NOT Worth It at This Scale)

### ~~Daily Revenue Rollups~~ *(DEPRIORITIZED)*
**Rationale**: With 2K orders/year, real-time aggregation is fast enough. Dashboard loads in <100ms.

### ~~Weekly/Monthly Summaries~~ *(DEPRIORITIZED)*  
**Rationale**: Simple date math in queries is sufficient for this data volume.

### ~~Product Performance Metrics~~ *(DEPRIORITIZED)*
**Rationale**: At this scale, ad-hoc product queries are manageable. Complex joins aren't slow enough to justify maintenance overhead.

### ~~Geographic Sales Analysis~~ *(DEPRIORITIZED)*
**Rationale**: Address normalization complexity not justified for small customer base. Better to handle in application if needed.

### ~~Order Status History~~ *(DEPRIORITIZED)*
**Rationale**: Status transitions analysis not critical for small volume. Add only if specific business need emerges.

## Data Quality Issues to Address

### ⚠️ Critical: Date Fields Returning as Strings
**Issue**: `order_date`, `due_date`, `ship_date` defined as `date` type but queried results return strings
**Impact**: Cannot use native Date operations, requires string manipulation in application
**DBT Fix**: Ensure date fields are properly typed in staging, investigate why dates are stringified
**Discovered**: During dashboard development - had to use string manipulation instead of Date objects

### Sales Price & Purchase Cost Data Types
**Issue**: `sales_price` and `purchase_cost` stored as TEXT instead of NUMERIC
**Impact**: Cannot perform margin calculations without type casting
**DBT Fix**: Cast to NUMERIC in staging layer, handle null/invalid values

### Order Status Standardization
**Issue**: `status` field contains free-text values (inconsistent casing, variations)
**Impact**: Complex conditional logic for status badges, unreliable status filtering
**DBT Fix**: Create `dim_order_statuses` lookup table, standardize to enum values
**Discovered**: During dashboard development - had to handle multiple status variations

### Currency Handling Inconsistency
**Issue**: `exchange_rate` field exists but `total_amount` not consistently in base currency
**Impact**: Revenue calculations may be inaccurate for multi-currency orders
**DBT Fix**: Standardize all monetary fields to base currency using exchange rates

### Missing Primary Keys and Constraints
**Issue**: No visible primary keys or foreign key relationships in schema
**Impact**: Potential duplicate records, unclear data relationships
**DBT Fix**: Add appropriate primary keys, foreign keys, and unique constraints

### Nullable Fields That Shouldn't Be
**Issue**: Critical fields like `order_number`, `total_amount` are nullable
**Impact**: Defensive programming required, potential null pointer errors
**DBT Fix**: Add NOT NULL constraints to essential business fields

### Field Naming Inconsistencies
**Issue**: Mix of camelCase (`quickBooksInternalId`) and snake_case (`quickbooks_internal_id`)
**Impact**: Confusing schema, potential mapping errors
**DBT Fix**: Standardize all field names to consistent convention (recommend snake_case)

### Missing Indexes for Dashboard Performance
**Issue**: No indexes on frequently queried columns (order_date, customer, status, total_amount)
**Impact**: Slow dashboard queries, poor user experience
**DBT Fix**: Add composite indexes for common query patterns:
- `(order_date, status)` for time-based status filtering  
- `(customer, order_date)` for customer analytics
- `(order_date, total_amount)` for revenue calculations

### Timestamp Fields Without Timezone
**Issue**: `created_date`, `modified_date` stored as timestamp without timezone
**Impact**: Ambiguous times, potential issues with multi-timezone business
**DBT Fix**: Convert to `timestamptz` with explicit timezone handling
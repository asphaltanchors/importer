# DBT Pipeline Candidates

This file tracks opportunities to move logic from the dashboard app into the DBT pipeline for better performance and maintainability.

**Context**: Small ecommerce site (~2,000 orders/year, max 10 line items/order = ~20,000 total records)
**Philosophy**: Balance complexity vs. optimization - real-time aggregation is fine for this scale, focus DBT efforts on data quality and business logic rather than performance optimization.

## High Priority (Data Quality & Business Logic)

### ✅ RESOLVED: Date Fields Returning as Strings *(FIXED)*
**Previous State**: Date fields defined as `date` but return strings in queries
**Resolution**: Date fields are now properly typed as DATE/TIMESTAMP in DBT pipeline
**Verification**: Native date operations work correctly, MCP server returns proper date objects
**Status**: Fixed in current DBT models - dates properly cast in `int_quickbooks__order_items_typed.sql`

### ✅ RESOLVED: Order Status Standardization *(FIXED)*
**Previous State**: Free-text status values with inconsistent casing (PAID vs Paid, etc.)
**Resolution**: Implemented standardized status mapping in intermediate layer
**Implementation**: Added CASE WHEN logic in `int_quickbooks__order_items_typed.sql` to normalize:
- 'PAID' (from both 'PAID' and 'Paid')
- 'OPEN' (from 'Open')
- 'PARTIALLY_PAID' (from 'Partially Paid')
**Benefits**: Consistent status values, reliable `is_paid` flag, cleaner UI logic

## Medium Priority (Business Logic Worth Centralizing)

### ✅ RESOLVED: Sales Price & Purchase Cost Typing *(FIXED)*
**Previous State**: Stored as TEXT, preventing margin calculations
**Resolution**: Implemented proper NUMERIC casting in staging layer with margin calculations
**Implementation**: Enhanced `stg_quickbooks__items.sql` with CAST to NUMERIC and added margin calculations to `fct_products.sql`
**Benefits**: Direct margin calculations (margin_percentage, margin_amount), proper arithmetic operations
**Result**: Average product margin of 66.28%, enables comprehensive profit analysis

### Customer Lifetime Value (CLV)
**Current State**: Not yet implemented
**DBT Candidate**: Create `dim_customer_metrics` with CLV, frequency, AOV
**Benefits**: Pre-calculated customer insights
**Complexity**: Medium - customer cohort analysis
**Rationale**: Complex calculation worth centralizing, but not urgent at this scale

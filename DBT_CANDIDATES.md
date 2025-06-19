# DBT Pipeline Candidates

This file tracks opportunities to move logic from the dashboard app into the DBT pipeline for better performance and maintainability.

**Context**: Small ecommerce site (~2,000 orders/year, max 10 line items/order = ~20,000 total records)
**Philosophy**: Balance complexity vs. optimization - real-time aggregation is fine for this scale, focus DBT efforts on data quality and business logic rather than performance optimization.

## Active Candidates

### Customer Lifetime Value (CLV)
**Current State**: Not yet implemented
**DBT Candidate**: Create `dim_customer_metrics` with CLV, frequency, AOV
**Benefits**: Pre-calculated customer insights
**Complexity**: Medium - customer cohort analysis
**Rationale**: Complex calculation worth centralizing, but not urgent at this scale

## Future Discoveries

### Product Sales Metrics View
**Discovered**: During products table enhancement (2024-12-18)
**Current State**: Complex JOIN query in application for trailing year sales aggregation
**Query Complexity**: LEFT JOIN between `fct_products` and aggregated `fct_order_line_items` data
**Current Implementation**: 
- Query: `getProducts()` and `getProductByName()` in lib/queries.ts:547-697
- Metrics: trailing year sales amount, units sold, order count
- Performance: Complex aggregation with GROUP BY and date filtering

**DBT Candidate**: Create `dim_products_with_sales_metrics` view
**Proposed Implementation**:
```sql
SELECT 
  p.*,
  COALESCE(sales.trailing_year_sales, 0) as trailing_year_sales,
  COALESCE(sales.trailing_year_units, 0) as trailing_year_units,
  COALESCE(sales.trailing_year_orders, 0) as trailing_year_orders
FROM {{ ref('fct_products') }} p
LEFT JOIN {{ ref('mart_product_trailing_sales') }} sales 
  ON p.item_name = sales.product_service
```

**Benefits**: 
- Pre-calculated sales metrics eliminate complex runtime JOINs
- Reusable across multiple features (product pages, reporting, analytics)
- Centralized business logic for "trailing year" definition
- Better query performance for product listings

**Priority**: Medium 
**Trigger Conditions**: 
- Query performance >2 seconds 
- Sales metrics needed in other features
- More complex sales analytics required (YoY growth, seasonality, etc.)

**Complexity**: Medium - requires new mart table with incremental refresh strategy for performance

### ✅ Company Analytics Implementation *(COMPLETED - December 2024)*
**Discovered**: During company dashboard development (December 2024)
**Resolution**: Successfully implemented comprehensive company analytics system addressing core business intelligence needs

**Implementation Results**:
- ✅ `fct_companies` - Existing company consolidation via email domains (2,594 companies)
- ✅ `fct_company_orders` - Existing order-level data with business classifications  
- ✅ `fct_company_products` - Existing product purchasing intelligence
- ✅ **NEW: `dim_company_health`** - Health indicators and activity scoring (2,594 records)
- ✅ **NEW: `fct_company_orders_time_series`** - Quarterly time-series for growth analysis (4,291 records)

**Key Features Delivered**:

1. **`dim_company_health`** - Comprehensive health scoring system
   - Health scores (0-100) based on recency, frequency, growth, engagement
   - Activity status (Highly Active → Inactive)
   - Growth trend analysis (Growing, Declining, Stable, New Customer, Lost Customer)
   - Risk and opportunity flags for account management
   - Days since last order tracking

2. **`fct_company_orders_time_series`** - Temporal business intelligence
   - Quarterly aggregations with YoY and QoQ growth metrics
   - Revenue and order count trend analysis
   - Growth classifications (High Growth, Moderate Growth, Declining, etc.)
   - Seasonal pattern tracking and current quarter identification
   - Exceptional growth and concerning decline flags

**Business Impact**:
- ✅ Company health scoring enables proactive account management
- ✅ Growth trend analysis supports strategic relationship decisions  
- ✅ Temporal data enables forecasting and lifecycle management
- ✅ Risk/opportunity flagging automates account prioritization
- ✅ All data quality tests passing with comprehensive schema documentation

**Code Quality**: 
- All models include comprehensive testing (uniqueness, not_null, accepted_values)
- Proper materialization as tables for performance
- Complete YAML documentation with descriptions
- Follows existing DBT project conventions and patterns

**Status**: Production-ready with 2,594 company health records and 4,291 time-series data points

*New candidates will be added here as the application grows and new data quality issues or business logic opportunities are discovered.*

## Resolved Items

### ✅ Date Fields Returning as Strings *(FIXED)*
**Previous State**: Date fields defined as `date` but return strings in queries
**Resolution**: Date fields are now properly typed as DATE/TIMESTAMP in DBT pipeline
**Status**: Fixed in current DBT models - dates properly cast in `int_quickbooks__order_items_typed.sql`
**Code Updated**: lib/queries.ts:34,38,122,151 (.toISOString().split('T')[0] removed), RevenueChart.tsx:22 (date conversion simplified)

### ✅ Order Status Standardization *(FIXED)*
**Previous State**: Free-text status values with inconsistent casing (PAID vs Paid, etc.)
**Resolution**: Implemented standardized status mapping in intermediate layer
**Implementation**: Added CASE WHEN logic in `int_quickbooks__order_items_typed.sql` to normalize:
- 'PAID' (from both 'PAID' and 'Paid')
- 'OPEN' (from 'Open')
- 'PARTIALLY_PAID' (from 'Partially Paid')
**Code Updated**: lib/queries.ts:113 (status fallback removed), RecentOrders.tsx:11-28 (StatusBadge simplified to direct matching)

### ✅ Sales Price & Purchase Cost Typing *(FIXED)*
**Previous State**: Stored as TEXT, preventing margin calculations
**Resolution**: Implemented proper NUMERIC casting in staging layer with margin calculations
**Implementation**: Enhanced `stg_quickbooks__items.sql` with CAST to NUMERIC and added margin calculations to `fct_products.sql`
**Result**: Average product margin of 66.28%, enables comprehensive profit analysis
**Code Updated**: lib/queries.ts:74,75,83,112,142 (parseFloat() removed), RevenueChart.tsx:26,30 (Number() conversion), RecentOrders.tsx:80 (parseFloat() removed)

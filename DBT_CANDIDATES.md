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

### Company Analytics Implementation
**Discovered**: During company dashboard development (December 2024)
**Current Issue**: Customer drill-down approach is misguided - "customers" within companies are just store locations or data entry artifacts
**User Insight**: "I don't really care about those customers. I care about active companies, order trends, product purchasing, company growth/decline"

**Current Schema Assessment**:
- ✅ `fct_companies` - Good company consolidation
- ✅ `fct_company_orders` - Has order-level data but lacks time-series aggregations
- ✅ `fct_company_products` - Has product purchasing data
- ❌ Missing: Company health indicators (active/inactive, days since last order)
- ❌ Missing: Time-series aggregations for growth analysis
- ❌ Missing: Year-over-year comparison capabilities

**DBT Enhancement Candidates**:

1. **`dim_company_health`** - Health and activity indicators
   ```sql
   SELECT 
     company_domain_key,
     days_since_last_order,
     order_frequency_category,
     activity_status,
     growth_trend_direction,
     health_score
   ```

2. **`fct_company_orders_time_series`** - Temporal analysis
   ```sql
   SELECT 
     company_domain_key,
     order_year,
     order_quarter,
     total_revenue,
     order_count,
     yoy_revenue_growth,
     yoy_order_growth
   ```

3. **Enhanced `fct_company_products`** - Product trend analysis
   - Add time-based purchasing patterns
   - Product category diversification metrics
   - Purchase volume trends

**Benefits**: 
- Shift from meaningless customer details to actionable company business intelligence
- Enable growth/decline assessment, health scoring, product intelligence
- Support company lifecycle management and relationship strength evaluation

**Priority**: High - Core business intelligence requirement
**Complexity**: Medium - Requires time-series calculations and health scoring logic

*New candidates will be added here as the application grows and new data quality issues or business logic opportunities are discovered.*

## Resolved Items

### ✅ Product Top Companies Period-Based Spending *(FIXED)*
**Previous State**: Complex 4-table join query failing due to relationship mismatches
**Failed Implementation**: `getTopCompaniesForProduct()` in lib/queries/companies.ts:637-686 blocking critical product analytics
**Root Problem**: Period-filtered spending required transaction-level aggregation, not pre-aggregated lifetime totals
**Business Need**: "Show me companies who bought Product X in the last 90 days and how much they spent in that period"

**Resolution**: Implemented `mart_product_company_period_spending` DBT table
**Implementation**: Created comprehensive period-based aggregation table with:
- **Multiple periods**: 30d, 90d, 1y, all_time in single table
- **Rich context**: Company details, product classifications, business categories
- **Smart architecture**: Leverages existing `fct_order_line_items`, `bridge_customer_company`, `fct_companies`, `fct_company_products`
- **Robust data quality**: 28 tests passing, proper NULL handling, edge case management
- **15,384 rows** across all product-company-period combinations

**Dashboard Integration**: Complex join eliminated, now simple query:
```sql
SELECT * FROM mart_product_company_period_spending 
WHERE product_service = ? AND period_type = 'trailing_90d'
ORDER BY total_amount_spent DESC LIMIT 10
```

**Status**: ✅ **RESOLVED** - Critical product analytics feature unblocked
**Date**: 2024-12-25

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

/*
  Company Orders Time Series Fact Table

  Aggregates company order data by time periods to enable temporal analysis,
  growth tracking, and year-over-year comparisons. Provides the foundation
  for trend analysis and company lifecycle management.
*/

{{ config(
    materialized = 'table',
    tags = ['companies', 'time_series', 'analytics']
) }}

WITH company_orders_by_period AS (
    SELECT 
        company_domain_key,
        order_year,
        order_quarter,
        
        -- Basic aggregations
        COUNT(*) as order_count,
        COUNT(DISTINCT customer_name) as unique_customers,
        SUM(calculated_order_total) as total_revenue,
        AVG(calculated_order_total) as avg_order_value,
        
        -- Product diversity
        SUM(unique_products) as total_unique_products,
        AVG(unique_products) as avg_products_per_order,
        
        -- Order characteristics
        COUNT(CASE WHEN order_size_category = 'Large Order ($10K+)' THEN 1 END) as large_orders,
        COUNT(CASE WHEN order_size_category = 'Medium Order ($2.5K-$10K)' THEN 1 END) as medium_orders,
        COUNT(CASE WHEN order_size_category = 'Small Order ($500-$2.5K)' THEN 1 END) as small_orders,
        COUNT(CASE WHEN order_size_category = 'Micro Order (<$500)' THEN 1 END) as micro_orders,
        
        -- Margin analysis (where available)
        AVG(avg_margin_percentage) as avg_margin_percentage,
        
        -- Date ranges
        MIN(order_date) as period_start_date,
        MAX(order_date) as period_end_date
        
    FROM {{ ref('fct_company_orders') }}
    GROUP BY company_domain_key, order_year, order_quarter
),

time_series_with_company_context AS (
    SELECT 
        cop.*,
        fc.company_name,
        fc.domain_type,
        fc.business_size_category,
        fc.revenue_category,
        
        -- Period identification
        cop.order_year || '-Q' || cop.order_quarter as period_key,
        
        -- Calculate previous period values for growth analysis
        LAG(cop.order_count) OVER (
            PARTITION BY cop.company_domain_key 
            ORDER BY cop.order_year, cop.order_quarter
        ) as prev_quarter_orders,
        
        LAG(cop.total_revenue) OVER (
            PARTITION BY cop.company_domain_key 
            ORDER BY cop.order_year, cop.order_quarter
        ) as prev_quarter_revenue,
        
        -- Calculate year-over-year values
        LAG(cop.order_count, 4) OVER (
            PARTITION BY cop.company_domain_key 
            ORDER BY cop.order_year, cop.order_quarter
        ) as yoy_prev_orders,
        
        LAG(cop.total_revenue, 4) OVER (
            PARTITION BY cop.company_domain_key 
            ORDER BY cop.order_year, cop.order_quarter
        ) as yoy_prev_revenue
        
    FROM company_orders_by_period cop
    INNER JOIN {{ ref('fct_companies') }} fc ON cop.company_domain_key = fc.company_domain_key
),

time_series_with_growth_metrics AS (
    SELECT 
        *,
        
        -- Quarter-over-quarter growth using standardized macro
        {{ calculate_growth_percentage('order_count', 'prev_quarter_orders') }} as qoq_order_growth_pct,
        {{ calculate_growth_percentage('total_revenue', 'prev_quarter_revenue') }} as qoq_revenue_growth_pct,
        
        -- Year-over-year growth using standardized macro
        {{ calculate_growth_percentage('order_count', 'yoy_prev_orders') }} as yoy_order_growth_pct,
        {{ calculate_growth_percentage('total_revenue', 'yoy_prev_revenue') }} as yoy_revenue_growth_pct,
        
        -- Growth trend classification
        CASE 
            WHEN yoy_prev_orders IS NULL THEN 'New Period'
            WHEN yoy_prev_orders = 0 AND order_count > 0 THEN 'New Activity'
            WHEN yoy_prev_orders > 0 AND order_count = 0 THEN 'Lost Activity'
            WHEN order_count > yoy_prev_orders * 1.5 THEN 'High Growth'
            WHEN order_count > yoy_prev_orders * 1.2 THEN 'Moderate Growth'
            WHEN order_count < yoy_prev_orders * 0.8 THEN 'Declining'
            WHEN order_count < yoy_prev_orders * 0.5 THEN 'Significant Decline'
            ELSE 'Stable'
        END as yoy_growth_trend,
        
        -- Revenue trend classification
        CASE 
            WHEN yoy_prev_revenue IS NULL THEN 'New Period'
            WHEN yoy_prev_revenue = 0 AND total_revenue > 0 THEN 'New Revenue'
            WHEN yoy_prev_revenue > 0 AND total_revenue = 0 THEN 'Lost Revenue'
            WHEN total_revenue > yoy_prev_revenue * 1.5 THEN 'High Revenue Growth'
            WHEN total_revenue > yoy_prev_revenue * 1.2 THEN 'Moderate Revenue Growth'
            WHEN total_revenue < yoy_prev_revenue * 0.8 THEN 'Revenue Declining'
            WHEN total_revenue < yoy_prev_revenue * 0.5 THEN 'Significant Revenue Decline'
            ELSE 'Stable Revenue'
        END as yoy_revenue_trend
        
    FROM time_series_with_company_context
)

SELECT 
    -- Identifiers
    company_domain_key,
    company_name,
    domain_type,
    business_size_category,
    revenue_category,
    
    -- Time dimensions
    order_year,
    order_quarter,
    period_key,
    period_start_date,
    period_end_date,
    
    -- Core metrics
    order_count,
    unique_customers,
    total_revenue,
    avg_order_value,
    
    -- Product metrics
    total_unique_products,
    avg_products_per_order,
    
    -- Order mix
    large_orders,
    medium_orders,
    small_orders,
    micro_orders,
    
    -- Profitability
    avg_margin_percentage,
    
    -- Growth metrics
    qoq_order_growth_pct,
    qoq_revenue_growth_pct,
    yoy_order_growth_pct,
    yoy_revenue_growth_pct,
    
    -- Trend classifications
    yoy_growth_trend,
    yoy_revenue_trend,
    
    -- Market share indicators
    CASE 
        WHEN total_revenue >= 50000 THEN 'Major Customer'
        WHEN total_revenue >= 10000 THEN 'Significant Customer'
        WHEN total_revenue >= 2500 THEN 'Regular Customer'
        ELSE 'Small Customer'
    END as quarterly_revenue_tier,
    
    -- Activity pattern
    CASE 
        WHEN order_count >= 10 THEN 'High Activity'
        WHEN order_count >= 5 THEN 'Medium Activity'
        WHEN order_count >= 2 THEN 'Low Activity'
        ELSE 'Minimal Activity'
    END as quarterly_activity_level,
    
    -- Seasonality flags
    CASE 
        WHEN order_quarter = 1 THEN 'Q1'
        WHEN order_quarter = 2 THEN 'Q2' 
        WHEN order_quarter = 3 THEN 'Q3'
        WHEN order_quarter = 4 THEN 'Q4'
    END as quarter_label,
    
    -- Recency flag
    CASE 
        WHEN order_year = EXTRACT(YEAR FROM CURRENT_DATE) 
         AND order_quarter = EXTRACT(QUARTER FROM CURRENT_DATE) THEN true
        ELSE false
    END as is_current_quarter,
    
    -- Performance indicators
    CASE 
        WHEN yoy_revenue_growth_pct > 50 AND total_revenue > 5000 THEN true
        WHEN yoy_order_growth_pct > 100 AND order_count > 2 THEN true
        ELSE false
    END as exceptional_growth_flag,
    
    CASE 
        WHEN yoy_revenue_growth_pct < -50 AND yoy_prev_revenue > 1000 THEN true
        WHEN yoy_order_growth_pct < -75 AND yoy_prev_orders > 1 THEN true
        ELSE false
    END as concerning_decline_flag,
    
    -- Metadata
    CURRENT_TIMESTAMP as created_at
    
FROM time_series_with_growth_metrics
ORDER BY company_domain_key, order_year DESC, order_quarter DESC
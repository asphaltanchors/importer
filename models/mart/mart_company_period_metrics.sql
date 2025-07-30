/*
ABOUTME: Company period-based metrics mart for dashboard analytics
ABOUTME: Pre-calculates revenue and order metrics by company across multiple time periods
*/

{{ config(
    materialized = 'table',
    tags = ['mart', 'companies', 'period_metrics', 'analytics']
) }}

WITH corporate_companies AS (
    -- Start with corporate companies only to match dashboard requirements
    SELECT *
    FROM {{ ref('fct_companies') }}
    WHERE domain_type = 'corporate'
),

company_order_base AS (
    -- Get all company orders for corporate companies
    SELECT 
        co.company_domain_key,
        co.order_date,
        co.calculated_order_total,
        co.customer_name,
        co.order_number
    FROM {{ ref('fct_company_orders') }} co
    INNER JOIN corporate_companies cc 
        ON co.company_domain_key = cc.company_domain_key
    WHERE co.calculated_order_total IS NOT NULL
      AND co.calculated_order_total > 0
      AND co.order_date IS NOT NULL
),

period_aggregations AS (
    SELECT 
        cob.company_domain_key,
        
        -- 7 day period metrics
        'trailing_7d' as period_type,
        SUM(CASE WHEN cob.order_date >= CURRENT_DATE - INTERVAL '7 days' 
            THEN cob.calculated_order_total ELSE 0 END) as total_revenue,
        COUNT(CASE WHEN cob.order_date >= CURRENT_DATE - INTERVAL '7 days' 
              THEN 1 ELSE NULL END) as total_orders,
        COUNT(DISTINCT CASE WHEN cob.order_date >= CURRENT_DATE - INTERVAL '7 days' 
                           THEN cob.customer_name ELSE NULL END) as customer_count,
        MIN(CASE WHEN cob.order_date >= CURRENT_DATE - INTERVAL '7 days' 
            THEN cob.order_date ELSE NULL END) as first_order_date,
        MAX(CASE WHEN cob.order_date >= CURRENT_DATE - INTERVAL '7 days' 
            THEN cob.order_date ELSE NULL END) as latest_order_date
        
    FROM company_order_base cob
    GROUP BY cob.company_domain_key
    
    UNION ALL
    
    SELECT 
        cob.company_domain_key,
        
        -- 30 day period metrics
        'trailing_30d' as period_type,
        SUM(CASE WHEN cob.order_date >= CURRENT_DATE - INTERVAL '30 days' 
            THEN cob.calculated_order_total ELSE 0 END) as total_revenue,
        COUNT(CASE WHEN cob.order_date >= CURRENT_DATE - INTERVAL '30 days' 
              THEN 1 ELSE NULL END) as total_orders,
        COUNT(DISTINCT CASE WHEN cob.order_date >= CURRENT_DATE - INTERVAL '30 days' 
                           THEN cob.customer_name ELSE NULL END) as customer_count,
        MIN(CASE WHEN cob.order_date >= CURRENT_DATE - INTERVAL '30 days' 
            THEN cob.order_date ELSE NULL END) as first_order_date,
        MAX(CASE WHEN cob.order_date >= CURRENT_DATE - INTERVAL '30 days' 
            THEN cob.order_date ELSE NULL END) as latest_order_date
        
    FROM company_order_base cob
    GROUP BY cob.company_domain_key
    
    UNION ALL
    
    SELECT 
        cob.company_domain_key,
        
        -- 90 day period metrics
        'trailing_90d' as period_type,
        SUM(CASE WHEN cob.order_date >= CURRENT_DATE - INTERVAL '90 days' 
            THEN cob.calculated_order_total ELSE 0 END) as total_revenue,
        COUNT(CASE WHEN cob.order_date >= CURRENT_DATE - INTERVAL '90 days' 
              THEN 1 ELSE NULL END) as total_orders,
        COUNT(DISTINCT CASE WHEN cob.order_date >= CURRENT_DATE - INTERVAL '90 days' 
                           THEN cob.customer_name ELSE NULL END) as customer_count,
        MIN(CASE WHEN cob.order_date >= CURRENT_DATE - INTERVAL '90 days' 
            THEN cob.order_date ELSE NULL END) as first_order_date,
        MAX(CASE WHEN cob.order_date >= CURRENT_DATE - INTERVAL '90 days' 
            THEN cob.order_date ELSE NULL END) as latest_order_date
        
    FROM company_order_base cob
    GROUP BY cob.company_domain_key
    
    UNION ALL
    
    SELECT 
        cob.company_domain_key,
        
        -- 1 year period metrics
        'trailing_1y' as period_type,
        SUM(CASE WHEN cob.order_date >= CURRENT_DATE - INTERVAL '1 year' 
            THEN cob.calculated_order_total ELSE 0 END) as total_revenue,
        COUNT(CASE WHEN cob.order_date >= CURRENT_DATE - INTERVAL '1 year' 
              THEN 1 ELSE NULL END) as total_orders,
        COUNT(DISTINCT CASE WHEN cob.order_date >= CURRENT_DATE - INTERVAL '1 year' 
                           THEN cob.customer_name ELSE NULL END) as customer_count,
        MIN(CASE WHEN cob.order_date >= CURRENT_DATE - INTERVAL '1 year' 
            THEN cob.order_date ELSE NULL END) as first_order_date,
        MAX(CASE WHEN cob.order_date >= CURRENT_DATE - INTERVAL '1 year' 
            THEN cob.order_date ELSE NULL END) as latest_order_date
        
    FROM company_order_base cob
    GROUP BY cob.company_domain_key
    
    UNION ALL
    
    SELECT 
        cob.company_domain_key,
        
        -- All time metrics
        'all_time' as period_type,
        SUM(cob.calculated_order_total) as total_revenue,
        COUNT(*) as total_orders,
        COUNT(DISTINCT cob.customer_name) as customer_count,
        MIN(cob.order_date) as first_order_date,
        MAX(cob.order_date) as latest_order_date
        
    FROM company_order_base cob
    GROUP BY cob.company_domain_key
),

-- Filter to only periods with actual activity
filtered_periods AS (
    SELECT *
    FROM period_aggregations
    WHERE total_revenue > 0 OR total_orders > 0
),

-- Enrich with company context
final_with_context AS (
    SELECT 
        pa.*,
        
        -- Company details
        cc.company_name,
        cc.domain_type,
        cc.business_size_category,
        cc.total_revenue as lifetime_total_revenue,
        cc.total_orders as lifetime_total_orders,
        cc.customer_count as lifetime_customer_count,
        
        -- Company classification
        cc.revenue_category as lifetime_revenue_category,
        cc.has_revenue,
        cc.is_multi_location,
        cc.is_corporate,
        
        -- Period-specific business classifications
        CASE 
            WHEN pa.latest_order_date >= CURRENT_DATE - INTERVAL '30 days' THEN 'Recent Activity (30d)'
            WHEN pa.latest_order_date >= CURRENT_DATE - INTERVAL '90 days' THEN 'Active (90d)'
            WHEN pa.latest_order_date >= CURRENT_DATE - INTERVAL '1 year' THEN 'Past Year Activity'
            ELSE 'Historical Activity'
        END as period_activity_status,
        
        CASE 
            WHEN pa.total_revenue >= 50000 THEN 'High Revenue ($50K+)'
            WHEN pa.total_revenue >= 10000 THEN 'Medium Revenue ($10K-$50K)'
            WHEN pa.total_revenue >= 2500 THEN 'Low Revenue ($2.5K-$10K)'
            WHEN pa.total_revenue > 0 THEN 'Minimal Revenue (<$2.5K)'
            ELSE 'No Period Revenue'
        END as period_revenue_category,
        
        -- Period share of lifetime activity
        CASE 
            WHEN cc.total_revenue > 0 AND pa.total_revenue IS NOT NULL THEN
                LEAST(100.0, ROUND(CAST(pa.total_revenue * 100.0 / cc.total_revenue AS NUMERIC), 2))
            WHEN pa.total_revenue > 0 THEN 100.0  -- If this is their only period, it's 100%
            ELSE 0.0  -- No revenue in this period
        END as period_share_of_lifetime_revenue,
        
        -- Days since latest order for this period (handle future dates)
        CASE 
            WHEN pa.latest_order_date IS NOT NULL THEN
                GREATEST(0, CURRENT_DATE - pa.latest_order_date)
            ELSE NULL
        END as days_since_latest_order,
        
        -- Metadata
        CURRENT_TIMESTAMP as created_at
        
    FROM filtered_periods pa
    INNER JOIN corporate_companies cc 
        ON pa.company_domain_key = cc.company_domain_key
)

SELECT * 
FROM final_with_context
-- Focus on meaningful activity
WHERE total_revenue > 0 OR total_orders > 0
ORDER BY 
    company_domain_key,
    CASE period_type
        WHEN 'trailing_7d' THEN 1
        WHEN 'trailing_30d' THEN 2
        WHEN 'trailing_90d' THEN 3
        WHEN 'trailing_1y' THEN 4
        WHEN 'all_time' THEN 5
    END,
    total_revenue DESC
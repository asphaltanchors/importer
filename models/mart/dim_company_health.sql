/*
  Company Health Dimension

  Calculates health indicators and activity scoring for consolidated companies.
  Provides business intelligence on company engagement, activity levels, and 
  relationship strength to support account management and growth strategies.
*/

{{ config(
    materialized = 'table',
    tags = ['companies', 'health', 'analytics']
) }}

WITH company_activity AS (
    SELECT 
        company_domain_key,
        
        -- Order activity metrics
        COUNT(*) as total_orders,
        COUNT(DISTINCT order_year) as active_years,
        MAX(order_date) as last_order_date,
        MIN(order_date) as first_order_date,
        SUM(calculated_order_total) as total_revenue,
        AVG(calculated_order_total) as avg_order_value,
        
        -- Recent activity (last 90 days)
        COUNT(CASE WHEN order_date >= CURRENT_DATE - INTERVAL '90 days' THEN 1 END) as orders_last_90_days,
        SUM(CASE WHEN order_date >= CURRENT_DATE - INTERVAL '90 days' THEN calculated_order_total ELSE 0 END) as revenue_last_90_days,
        
        -- Year-over-year activity  
        COUNT(CASE WHEN order_date >= CURRENT_DATE - INTERVAL '1 year' THEN 1 END) as orders_last_year,
        COUNT(CASE WHEN order_date >= CURRENT_DATE - INTERVAL '2 years' AND order_date < CURRENT_DATE - INTERVAL '1 year' THEN 1 END) as orders_prior_year,
        
        -- Product diversity
        COUNT(DISTINCT unique_products) as product_diversity_score,
        
        -- Order frequency analysis
        COUNT(*) / GREATEST((MAX(order_date) - MIN(order_date)) / 365.0, 0.1) as orders_per_year
        
    FROM {{ ref('fct_company_orders') }}
    GROUP BY company_domain_key
),

company_health_metrics AS (
    SELECT 
        ca.*,
        fc.company_name,
        fc.domain_type,
        fc.business_size_category,
        fc.revenue_category,
        
        -- Days since last activity
        CURRENT_DATE - ca.last_order_date as days_since_last_order,
        
        -- Activity status classification
        CASE 
            WHEN ca.last_order_date >= CURRENT_DATE - INTERVAL '30 days' THEN 'Highly Active'
            WHEN ca.last_order_date >= CURRENT_DATE - INTERVAL '90 days' THEN 'Active'
            WHEN ca.last_order_date >= CURRENT_DATE - INTERVAL '180 days' THEN 'Moderately Active'
            WHEN ca.last_order_date >= CURRENT_DATE - INTERVAL '1 year' THEN 'Dormant'
            ELSE 'Inactive'
        END as activity_status,
        
        -- Order frequency classification
        CASE 
            WHEN ca.orders_per_year >= 12 THEN 'Monthly+'
            WHEN ca.orders_per_year >= 6 THEN 'Bi-Monthly'
            WHEN ca.orders_per_year >= 4 THEN 'Quarterly'
            WHEN ca.orders_per_year >= 2 THEN 'Bi-Annual'
            WHEN ca.orders_per_year >= 1 THEN 'Annual'
            ELSE 'Sporadic'
        END as order_frequency_category,
        
        -- Growth trend analysis
        CASE 
            WHEN ca.orders_prior_year = 0 AND ca.orders_last_year > 0 THEN 'New Customer'
            WHEN ca.orders_prior_year > 0 AND ca.orders_last_year = 0 THEN 'Lost Customer'
            WHEN ca.orders_last_year > ca.orders_prior_year * 1.2 THEN 'Growing'
            WHEN ca.orders_last_year < ca.orders_prior_year * 0.8 THEN 'Declining'
            ELSE 'Stable'
        END as growth_trend_direction,
        
        -- Engagement level
        CASE 
            WHEN ca.revenue_last_90_days > ca.avg_order_value * 2 THEN 'High Engagement'
            WHEN ca.orders_last_90_days > 0 THEN 'Medium Engagement'
            WHEN ca.last_order_date >= CURRENT_DATE - INTERVAL '180 days' THEN 'Low Engagement'
            ELSE 'No Recent Engagement'
        END as engagement_level
        
    FROM company_activity ca
    INNER JOIN {{ ref('fct_companies') }} fc ON ca.company_domain_key = fc.company_domain_key
),

health_scoring AS (
    SELECT 
        *,
        
        -- Health score calculation (0-100)
        ROUND(
            LEAST(100, 
                -- Recency component (40% weight)
                (CASE 
                    WHEN days_since_last_order <= 30 THEN 40
                    WHEN days_since_last_order <= 90 THEN 30
                    WHEN days_since_last_order <= 180 THEN 20
                    WHEN days_since_last_order <= 365 THEN 10
                    ELSE 0
                END) +
                
                -- Frequency component (30% weight)
                (CASE 
                    WHEN orders_per_year >= 12 THEN 30
                    WHEN orders_per_year >= 6 THEN 25
                    WHEN orders_per_year >= 4 THEN 20
                    WHEN orders_per_year >= 2 THEN 15
                    WHEN orders_per_year >= 1 THEN 10
                    ELSE 5
                END) +
                
                -- Growth component (20% weight)
                (CASE 
                    WHEN growth_trend_direction = 'Growing' THEN 20
                    WHEN growth_trend_direction = 'New Customer' THEN 15
                    WHEN growth_trend_direction = 'Stable' THEN 10
                    WHEN growth_trend_direction = 'Declining' THEN 5
                    ELSE 0
                END) +
                
                -- Engagement component (10% weight)
                (CASE 
                    WHEN engagement_level = 'High Engagement' THEN 10
                    WHEN engagement_level = 'Medium Engagement' THEN 7
                    WHEN engagement_level = 'Low Engagement' THEN 3
                    ELSE 0
                END)
            ), 
            0
        ) as health_score
        
    FROM company_health_metrics
)

SELECT 
    company_domain_key,
    company_name,
    domain_type,
    business_size_category,
    revenue_category,
    
    -- Core health metrics
    health_score,
    activity_status,
    engagement_level,
    growth_trend_direction,
    order_frequency_category,
    
    -- Key indicators
    days_since_last_order,
    last_order_date,
    first_order_date,
    
    -- Activity metrics
    total_orders,
    active_years,
    orders_per_year,
    total_revenue,
    avg_order_value,
    
    -- Recent performance
    orders_last_90_days,
    revenue_last_90_days,
    orders_last_year,
    orders_prior_year,
    
    -- Engagement indicators
    product_diversity_score,
    
    -- Health category
    CASE 
        WHEN health_score >= 80 THEN 'Excellent Health'
        WHEN health_score >= 60 THEN 'Good Health' 
        WHEN health_score >= 40 THEN 'Fair Health'
        WHEN health_score >= 20 THEN 'Poor Health'
        ELSE 'Critical Health'
    END as health_category,
    
    -- Risk flags
    CASE 
        WHEN days_since_last_order > 365 THEN true
        WHEN growth_trend_direction = 'Lost Customer' THEN true
        WHEN orders_last_year = 0 AND orders_prior_year > 0 THEN true
        ELSE false
    END as at_risk_flag,
    
    -- Opportunity flags
    CASE 
        WHEN growth_trend_direction = 'Growing' AND health_score >= 70 THEN true
        WHEN engagement_level = 'High Engagement' THEN true
        WHEN orders_per_year >= 6 AND avg_order_value > 1000 THEN true
        ELSE false
    END as growth_opportunity_flag,
    
    -- Metadata
    CURRENT_TIMESTAMP as created_at
    
FROM health_scoring
ORDER BY health_score DESC, total_revenue DESC
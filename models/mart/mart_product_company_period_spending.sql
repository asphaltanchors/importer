/*
ABOUTME: Product-company period-based spending mart for dashboard analytics
ABOUTME: Pre-calculates spending metrics by product and company across multiple time periods
*/

{{ config(
    materialized = 'table',
    tags = ['mart', 'products', 'companies', 'period_spending', 'analytics']
) }}

WITH transaction_base AS (
    SELECT 
        oli.product_service,
        bc.company_domain_key,
        fc.company_name,
        oli.order_date,
        oli.product_service_amount,
        oli.product_service_quantity,
        oli.product_service_rate,
        oli.customer,
        oli.order_number,
        oli.source_type,
        
        -- Window function for latest transaction info per product-company
        ROW_NUMBER() OVER (
            PARTITION BY oli.product_service, bc.company_domain_key 
            ORDER BY oli.order_date DESC
        ) as latest_transaction_rank
        
    FROM {{ ref('fct_order_line_items') }} oli
    INNER JOIN {{ ref('bridge_customer_company') }} bc 
        ON oli.customer = bc.customer_name
    INNER JOIN {{ ref('fct_companies') }} fc 
        ON bc.company_domain_key = fc.company_domain_key
    WHERE oli.product_service_amount IS NOT NULL
      AND oli.product_service_amount > 0
      AND oli.product_service IS NOT NULL
      AND TRIM(oli.product_service) != ''
      AND bc.company_domain_key IS NOT NULL
      AND bc.company_domain_key != 'NO_EMAIL_DOMAIN'
),

period_aggregations AS (
    SELECT 
        product_service,
        company_domain_key,
        company_name,
        
        -- 30 day period metrics
        'trailing_30d' as period_type,
        SUM(CASE WHEN order_date >= CURRENT_DATE - INTERVAL '30 days' 
            THEN product_service_amount ELSE 0 END) as total_amount_spent,
        COUNT(CASE WHEN order_date >= CURRENT_DATE - INTERVAL '30 days' 
              THEN 1 ELSE NULL END) as total_transactions,
        SUM(CASE WHEN order_date >= CURRENT_DATE - INTERVAL '30 days' 
            THEN COALESCE(product_service_quantity, 0) ELSE 0 END) as total_quantity_purchased,
        AVG(CASE WHEN order_date >= CURRENT_DATE - INTERVAL '30 days' 
            THEN product_service_rate ELSE NULL END) as avg_unit_price,
        MIN(order_date) as first_purchase_date,
        MAX(order_date) as last_purchase_date,
        -- Latest transaction info (for buyer status context)
        MAX(CASE WHEN latest_transaction_rank = 1 THEN customer END) as latest_customer,
        MAX(CASE WHEN latest_transaction_rank = 1 THEN source_type END) as latest_source_type
        
    FROM transaction_base
    GROUP BY product_service, company_domain_key, company_name
    
    UNION ALL
    
    SELECT 
        product_service,
        company_domain_key,
        company_name,
        
        -- 90 day period metrics
        'trailing_90d' as period_type,
        SUM(CASE WHEN order_date >= CURRENT_DATE - INTERVAL '90 days' 
            THEN product_service_amount ELSE 0 END) as total_amount_spent,
        COUNT(CASE WHEN order_date >= CURRENT_DATE - INTERVAL '90 days' 
              THEN 1 ELSE NULL END) as total_transactions,
        SUM(CASE WHEN order_date >= CURRENT_DATE - INTERVAL '90 days' 
            THEN COALESCE(product_service_quantity, 0) ELSE 0 END) as total_quantity_purchased,
        AVG(CASE WHEN order_date >= CURRENT_DATE - INTERVAL '90 days' 
            THEN product_service_rate ELSE NULL END) as avg_unit_price,
        MIN(order_date) as first_purchase_date,
        MAX(order_date) as last_purchase_date,
        MAX(CASE WHEN latest_transaction_rank = 1 THEN customer END) as latest_customer,
        MAX(CASE WHEN latest_transaction_rank = 1 THEN source_type END) as latest_source_type
        
    FROM transaction_base
    GROUP BY product_service, company_domain_key, company_name
    
    UNION ALL
    
    SELECT 
        product_service,
        company_domain_key,
        company_name,
        
        -- 1 year period metrics
        'trailing_1y' as period_type,
        SUM(CASE WHEN order_date >= CURRENT_DATE - INTERVAL '1 year' 
            THEN product_service_amount ELSE 0 END) as total_amount_spent,
        COUNT(CASE WHEN order_date >= CURRENT_DATE - INTERVAL '1 year' 
              THEN 1 ELSE NULL END) as total_transactions,
        SUM(CASE WHEN order_date >= CURRENT_DATE - INTERVAL '1 year' 
            THEN COALESCE(product_service_quantity, 0) ELSE 0 END) as total_quantity_purchased,
        AVG(CASE WHEN order_date >= CURRENT_DATE - INTERVAL '1 year' 
            THEN product_service_rate ELSE NULL END) as avg_unit_price,
        MIN(order_date) as first_purchase_date,
        MAX(order_date) as last_purchase_date,
        MAX(CASE WHEN latest_transaction_rank = 1 THEN customer END) as latest_customer,
        MAX(CASE WHEN latest_transaction_rank = 1 THEN source_type END) as latest_source_type
        
    FROM transaction_base
    GROUP BY product_service, company_domain_key, company_name
    
    UNION ALL
    
    SELECT 
        product_service,
        company_domain_key,
        company_name,
        
        -- All time metrics
        'all_time' as period_type,
        SUM(product_service_amount) as total_amount_spent,
        COUNT(*) as total_transactions,
        SUM(COALESCE(product_service_quantity, 0)) as total_quantity_purchased,
        AVG(product_service_rate) as avg_unit_price,
        MIN(order_date) as first_purchase_date,
        MAX(order_date) as last_purchase_date,
        MAX(CASE WHEN latest_transaction_rank = 1 THEN customer END) as latest_customer,
        MAX(CASE WHEN latest_transaction_rank = 1 THEN source_type END) as latest_source_type
        
    FROM transaction_base
    GROUP BY product_service, company_domain_key, company_name
),

-- Filter to only periods with actual spending
filtered_periods AS (
    SELECT *
    FROM period_aggregations
    WHERE total_amount_spent > 0
),

-- Enrich with existing business context
final_with_context AS (
    SELECT 
        pa.*,
        
        -- Company details from fct_companies
        fc.domain_type,
        fc.business_size_category,
        fc.revenue_category as company_revenue_category,
        fc.primary_country,
        fc.region,
        fc.customer_count as company_customer_count,
        fc.total_revenue as company_total_revenue,
        
        -- Product details from fct_products
        fp.product_family,
        fp.material_type,
        fp.is_kit,
        fp.item_type,
        fp.sales_price as standard_sales_price,
        fp.purchase_cost as standard_purchase_cost,
        fp.margin_percentage as standard_margin_percentage,
        
        -- Business context from fct_company_products (lifetime metrics)
        fcp.buyer_status as lifetime_buyer_status,
        fcp.purchase_volume_category as lifetime_volume_category,
        fcp.purchase_frequency_category as lifetime_frequency_category,
        fcp.total_amount_spent as lifetime_total_spent,
        fcp.total_transactions as lifetime_total_transactions,
        
        -- Period-specific business classifications
        CASE 
            WHEN pa.last_purchase_date >= CURRENT_DATE - INTERVAL '30 days' THEN 'Recent Buyer (30d)'
            WHEN pa.last_purchase_date >= CURRENT_DATE - INTERVAL '90 days' THEN 'Active Buyer (90d)'
            WHEN pa.last_purchase_date >= CURRENT_DATE - INTERVAL '1 year' THEN 'Past Year Buyer'
            ELSE 'Historical Buyer'
        END as period_buyer_status,
        
        CASE 
            WHEN pa.total_amount_spent >= 10000 THEN 'High Spender ($10K+)'
            WHEN pa.total_amount_spent >= 2500 THEN 'Medium Spender ($2.5K-$10K)'
            WHEN pa.total_amount_spent >= 500 THEN 'Low Spender ($500-$2.5K)'
            WHEN pa.total_amount_spent > 0 THEN 'Minimal Spender (<$500)'
            ELSE 'No Period Spending'
        END as period_spending_category,
        
        -- Price variance analysis (period vs standard)
        CASE 
            WHEN fp.sales_price > 0 AND pa.avg_unit_price > 0 THEN
                ROUND((pa.avg_unit_price - fp.sales_price) * 100.0 / fp.sales_price, 2)
            ELSE NULL
        END as price_variance_percentage,
        
        -- Period efficiency vs lifetime
        CASE 
            WHEN fcp.total_amount_spent > 0 AND pa.total_amount_spent IS NOT NULL THEN
                LEAST(100.0, ROUND(pa.total_amount_spent * 100.0 / fcp.total_amount_spent, 2))
            WHEN pa.total_amount_spent > 0 THEN 100.0  -- If this is their only period, it's 100%
            ELSE 0.0  -- No spending in this period
        END as period_share_of_lifetime_spending,
        
        -- Days since last purchase for this product (handle future dates)
        GREATEST(0, CURRENT_DATE - pa.last_purchase_date) as days_since_last_purchase,
        
        -- Metadata
        CURRENT_TIMESTAMP as created_at
        
    FROM filtered_periods pa
    INNER JOIN {{ ref('fct_companies') }} fc 
        ON pa.company_domain_key = fc.company_domain_key
    LEFT JOIN {{ ref('fct_products') }} fp 
        ON pa.product_service = fp.item_name
    LEFT JOIN {{ ref('fct_company_products') }} fcp 
        ON pa.company_domain_key = fcp.company_domain_key 
        AND pa.product_service = fcp.product_service
)

SELECT * 
FROM final_with_context
-- Focus on meaningful spending relationships
WHERE total_amount_spent > 0
ORDER BY 
    product_service,
    period_type,
    total_amount_spent DESC,
    company_name
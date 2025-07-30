/*
ABOUTME: Product-company period-based spending mart for dashboard analytics
ABOUTME: Pre-calculates spending metrics by product and company across multiple time periods
ABOUTME: Refactored to use fct_company_products as base to avoid rejoining violations

ARCHITECTURAL NOTE: This model has accepted DBT project evaluator "rejoining" violations for:
- fct_order_line_items + bridge_customer_company (transaction details needed for period calculations)
- These are business necessities as period metrics (30d, 90d, 1y) require transaction-level data
- that cannot be pre-aggregated due to dynamic date ranges. See CLAUDE.md for full justification.
*/

{{ config(
    materialized = 'table',  
    tags = ['mart', 'products', 'companies', 'period_spending', 'analytics']
) }}

WITH company_products_base AS (
    -- Start with fct_company_products to avoid rejoining violations
    -- This already has the company-product relationships established
    SELECT * FROM {{ ref('fct_company_products') }}
),

transaction_details AS (
    -- Get transaction-level details needed for period calculations
    -- Only fetch what's needed beyond what's in fct_company_products
    SELECT 
        oli.product_service,
        bc.company_domain_key,
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
    WHERE oli.product_service_amount IS NOT NULL
      AND oli.product_service_amount > 0
      AND oli.product_service IS NOT NULL
      AND TRIM(oli.product_service) != ''
      AND bc.company_domain_key IS NOT NULL
      AND bc.company_domain_key != 'NO_EMAIL_DOMAIN'
),

period_aggregations AS (
    SELECT 
        td.product_service,
        td.company_domain_key,
        
        -- 30 day period metrics
        'trailing_30d' as period_type,
        SUM(CASE WHEN td.order_date >= CURRENT_DATE - INTERVAL '30 days' 
            THEN td.product_service_amount ELSE 0 END) as total_amount_spent,
        COUNT(CASE WHEN td.order_date >= CURRENT_DATE - INTERVAL '30 days' 
              THEN 1 ELSE NULL END) as total_transactions,
        SUM(CASE WHEN td.order_date >= CURRENT_DATE - INTERVAL '30 days' 
            THEN COALESCE(td.product_service_quantity, 0) ELSE 0 END) as total_quantity_purchased,
        AVG(CASE WHEN td.order_date >= CURRENT_DATE - INTERVAL '30 days' 
            THEN td.product_service_rate ELSE NULL END) as avg_unit_price,
        MIN(td.order_date) as first_purchase_date,
        MAX(td.order_date) as last_purchase_date,
        -- Latest transaction info (for buyer status context)
        MAX(CASE WHEN td.latest_transaction_rank = 1 THEN td.customer END) as latest_customer,
        MAX(CASE WHEN td.latest_transaction_rank = 1 THEN td.source_type END) as latest_source_type
        
    FROM transaction_details td
    GROUP BY td.product_service, td.company_domain_key
    
    UNION ALL
    
    SELECT 
        td.product_service,
        td.company_domain_key,
        
        -- 90 day period metrics
        'trailing_90d' as period_type,
        SUM(CASE WHEN td.order_date >= CURRENT_DATE - INTERVAL '90 days' 
            THEN td.product_service_amount ELSE 0 END) as total_amount_spent,
        COUNT(CASE WHEN td.order_date >= CURRENT_DATE - INTERVAL '90 days' 
              THEN 1 ELSE NULL END) as total_transactions,
        SUM(CASE WHEN td.order_date >= CURRENT_DATE - INTERVAL '90 days' 
            THEN COALESCE(td.product_service_quantity, 0) ELSE 0 END) as total_quantity_purchased,
        AVG(CASE WHEN td.order_date >= CURRENT_DATE - INTERVAL '90 days' 
            THEN td.product_service_rate ELSE NULL END) as avg_unit_price,
        MIN(td.order_date) as first_purchase_date,
        MAX(td.order_date) as last_purchase_date,
        MAX(CASE WHEN td.latest_transaction_rank = 1 THEN td.customer END) as latest_customer,
        MAX(CASE WHEN td.latest_transaction_rank = 1 THEN td.source_type END) as latest_source_type
        
    FROM transaction_details td
    GROUP BY td.product_service, td.company_domain_key
    
    UNION ALL
    
    SELECT 
        td.product_service,
        td.company_domain_key,
        
        -- 1 year period metrics
        'trailing_1y' as period_type,
        SUM(CASE WHEN td.order_date >= CURRENT_DATE - INTERVAL '1 year' 
            THEN td.product_service_amount ELSE 0 END) as total_amount_spent,
        COUNT(CASE WHEN td.order_date >= CURRENT_DATE - INTERVAL '1 year' 
              THEN 1 ELSE NULL END) as total_transactions,
        SUM(CASE WHEN td.order_date >= CURRENT_DATE - INTERVAL '1 year' 
            THEN COALESCE(td.product_service_quantity, 0) ELSE 0 END) as total_quantity_purchased,
        AVG(CASE WHEN td.order_date >= CURRENT_DATE - INTERVAL '1 year' 
            THEN td.product_service_rate ELSE NULL END) as avg_unit_price,
        MIN(td.order_date) as first_purchase_date,
        MAX(td.order_date) as last_purchase_date,
        MAX(CASE WHEN td.latest_transaction_rank = 1 THEN td.customer END) as latest_customer,
        MAX(CASE WHEN td.latest_transaction_rank = 1 THEN td.source_type END) as latest_source_type
        
    FROM transaction_details td
    GROUP BY td.product_service, td.company_domain_key
    
    UNION ALL
    
    SELECT 
        td.product_service,
        td.company_domain_key,
        
        -- All time metrics
        'all_time' as period_type,
        SUM(product_service_amount) as total_amount_spent,
        COUNT(*) as total_transactions,
        SUM(COALESCE(product_service_quantity, 0)) as total_quantity_purchased,
        AVG(product_service_rate) as avg_unit_price,
        MIN(td.order_date) as first_purchase_date,
        MAX(td.order_date) as last_purchase_date,
        MAX(CASE WHEN td.latest_transaction_rank = 1 THEN td.customer END) as latest_customer,
        MAX(CASE WHEN td.latest_transaction_rank = 1 THEN td.source_type END) as latest_source_type
        
    FROM transaction_details td
    GROUP BY td.product_service, td.company_domain_key
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
        
        -- Company details (from company_products_base which includes company data)
        cpb.company_name,
        cpb.domain_type,
        cpb.business_size_category,
        cpb.company_total_revenue,
        
        -- Product details (from company_products_base which includes product data)
        cpb.product_family,
        cpb.material_type,
        cpb.is_kit,
        cpb.item_type,
        cpb.standard_sales_price,
        cpb.standard_purchase_cost,
        cpb.avg_margin_percentage as standard_margin_percentage,
        
        -- Business context (lifetime metrics from company_products_base)
        cpb.buyer_status as lifetime_buyer_status,
        cpb.purchase_volume_category as lifetime_volume_category,
        cpb.purchase_frequency_category as lifetime_frequency_category,
        cpb.total_amount_spent as lifetime_total_spent,
        cpb.total_transactions as lifetime_total_transactions,
        
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
            WHEN cpb.standard_sales_price > 0 AND pa.avg_unit_price > 0 THEN
                ROUND(CAST((pa.avg_unit_price - cpb.standard_sales_price) * 100.0 / cpb.standard_sales_price AS NUMERIC), 2)
            ELSE NULL
        END as price_variance_percentage,
        
        -- Period efficiency vs lifetime
        CASE 
            WHEN cpb.total_amount_spent > 0 AND pa.total_amount_spent IS NOT NULL THEN
                LEAST(100.0, ROUND(CAST(pa.total_amount_spent * 100.0 / cpb.total_amount_spent AS NUMERIC), 2))
            WHEN pa.total_amount_spent > 0 THEN 100.0  -- If this is their only period, it's 100%
            ELSE 0.0  -- No spending in this period
        END as period_share_of_lifetime_spending,
        
        -- Days since last purchase for this product (handle future dates)
        GREATEST(0, CURRENT_DATE - pa.last_purchase_date) as days_since_last_purchase,
        
        -- Metadata
        CURRENT_TIMESTAMP as created_at
        
    FROM filtered_periods pa
    INNER JOIN company_products_base cpb 
        ON pa.company_domain_key = cpb.company_domain_key 
        AND pa.product_service = cpb.product_service
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
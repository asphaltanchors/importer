/*
  Company-Product Analytics
  
  Aggregates what products/services each company buys, with quantities,
  frequencies, and purchasing patterns. Enables "who buys what" analysis
  at the product level.
*/

{{ config(
    materialized = 'table',
    tags = ['companies', 'products', 'analytics', 'quickbooks']
) }}

WITH company_product_details AS (
    SELECT 
        -- Link to companies via customer mapping - use clean line items data
        bc.company_domain_key,
        
        -- Product information (already cleaned in fct_order_line_items)
        oli.product_service,
        oli.product_service_description,
        
        -- Transaction details (already cleaned and typed)
        oli.order_date as transaction_date,
        oli.product_service_quantity as quantity,
        oli.product_service_rate as unit_price,
        oli.product_service_amount as line_amount,
        
        -- Context
        oli.sales_rep,
        oli.source_type
        
    FROM {{ ref('fct_order_line_items') }} oli
    INNER JOIN {{ ref('bridge_customer_company') }} bc 
        ON oli.customer = bc.customer_name
    WHERE oli.product_service_amount IS NOT NULL 
      AND oli.product_service_amount > 0
      AND oli.product_service IS NOT NULL
      AND TRIM(oli.product_service) != ''
),

-- Aggregate by company and product
company_product_metrics AS (
    SELECT 
        cpd.company_domain_key,
        cpd.product_service,
        -- Get the most recent product description for this company-product combination
        (SELECT cpd2.product_service_description 
         FROM company_product_details cpd2 
         WHERE cpd2.company_domain_key = cpd.company_domain_key 
           AND cpd2.product_service = cpd.product_service 
         ORDER BY cpd2.transaction_date DESC 
         LIMIT 1) as product_service_description,
        
        -- Purchase metrics
        COUNT(*) as total_transactions,
        COUNT(DISTINCT cpd.transaction_date) as purchase_days,
        SUM(cpd.quantity) as total_quantity_purchased,
        SUM(cpd.line_amount) as total_amount_spent,
        AVG(cpd.unit_price) as avg_unit_price,
        MIN(cpd.unit_price) as min_unit_price,
        MAX(cpd.unit_price) as max_unit_price,
        
        -- Timing metrics
        MIN(cpd.transaction_date) as first_purchase_date,
        MAX(cpd.transaction_date) as latest_purchase_date,
        COUNT(CASE WHEN cpd.transaction_date >= CURRENT_DATE - INTERVAL '90 days' THEN 1 END) as recent_transactions,
        COUNT(CASE WHEN cpd.transaction_date >= CURRENT_DATE - INTERVAL '1 year' THEN 1 END) as last_year_transactions,
        
        -- Sales rep
        MODE() WITHIN GROUP (ORDER BY cpd.sales_rep) as primary_sales_rep,
        
        -- Product enrichment from fct_products
        MAX(p.product_family) as product_family,
        MAX(p.material_type) as material_type,
        BOOL_OR(p.is_kit) as is_kit,  -- Use BOOL_OR for boolean aggregation
        MAX(p.item_type) as item_type,
        MAX(p.sales_price) as standard_sales_price,
        MAX(p.purchase_cost) as standard_purchase_cost
        
    FROM company_product_details cpd
    LEFT JOIN {{ ref('fct_products') }} p ON cpd.product_service = p.item_name
    WHERE cpd.company_domain_key != 'NO_EMAIL_DOMAIN'
    GROUP BY cpd.company_domain_key, cpd.product_service
)

SELECT 
    -- Company and product identification
    cpm.company_domain_key,
    fc.company_name,
    fc.domain_type,
    fc.business_size_category,
    cpm.product_service,
    cpm.product_service_description,
    cpm.product_family,
    cpm.material_type,
    cpm.is_kit,
    cpm.item_type,
    
    -- Purchase volume metrics
    cpm.total_transactions,
    cpm.purchase_days,
    cpm.total_quantity_purchased,
    cpm.total_amount_spent,
    
    -- Pricing metrics
    cpm.avg_unit_price,
    cpm.min_unit_price,
    cpm.max_unit_price,
    cpm.standard_sales_price,
    cpm.standard_purchase_cost,
    
    -- Price variance analysis
    CASE 
        WHEN cpm.standard_sales_price > 0 AND cpm.avg_unit_price > 0 THEN
            ROUND((cpm.avg_unit_price - cpm.standard_sales_price) * 100.0 / cpm.standard_sales_price, 2)
        ELSE NULL
    END as price_variance_percentage,
    
    -- Margin analysis
    CASE 
        WHEN cpm.standard_purchase_cost > 0 AND cpm.avg_unit_price > 0 THEN
            ROUND((cpm.avg_unit_price - cpm.standard_purchase_cost) * 100.0 / cpm.avg_unit_price, 2)
        ELSE NULL
    END as avg_margin_percentage,
    
    -- Timing metrics
    cpm.first_purchase_date,
    cpm.latest_purchase_date,
    cpm.recent_transactions,
    cpm.last_year_transactions,
    CURRENT_DATE - cpm.latest_purchase_date as days_since_last_purchase,
    
    -- Purchase frequency
    CASE 
        WHEN cpm.purchase_days > 0 THEN 
            ROUND(CAST(cpm.total_transactions AS NUMERIC) / CAST(cpm.purchase_days AS NUMERIC), 2)
        ELSE 0
    END as transactions_per_purchase_day,
    
    -- Loyalty/consistency metrics
    CASE 
        WHEN cpm.latest_purchase_date >= CURRENT_DATE - INTERVAL '90 days' THEN 'Active Buyer'
        WHEN cpm.latest_purchase_date >= CURRENT_DATE - INTERVAL '1 year' THEN 'Recent Buyer'
        WHEN cpm.latest_purchase_date >= CURRENT_DATE - INTERVAL '2 years' THEN 'Dormant Buyer'
        ELSE 'Inactive Buyer'
    END as buyer_status,
    
    -- Purchase size classification
    CASE 
        WHEN cpm.total_amount_spent >= 10000 THEN 'High Volume ($10K+)'
        WHEN cpm.total_amount_spent >= 2500 THEN 'Medium Volume ($2.5K-$10K)'
        WHEN cpm.total_amount_spent >= 500 THEN 'Low Volume ($500-$2.5K)'
        ELSE 'Occasional (<$500)'
    END as purchase_volume_category,
    
    -- Purchase frequency classification
    CASE 
        WHEN cpm.total_transactions >= 20 THEN 'Frequent Buyer (20+ orders)'
        WHEN cpm.total_transactions >= 10 THEN 'Regular Buyer (10-19 orders)'
        WHEN cpm.total_transactions >= 5 THEN 'Occasional Buyer (5-9 orders)'
        ELSE 'Rare Buyer (<5 orders)'
    END as purchase_frequency_category,
    
    -- Company context
    fc.total_revenue as company_total_revenue,
    ROUND(cpm.total_amount_spent * 100.0 / NULLIF(fc.total_revenue, 0), 2) as product_revenue_percentage,
    
    -- Sales context
    cpm.primary_sales_rep,
    
    -- Metadata
    CURRENT_TIMESTAMP as created_at
    
FROM company_product_metrics cpm
INNER JOIN {{ ref('fct_companies') }} fc ON cpm.company_domain_key = fc.company_domain_key
ORDER BY cpm.total_amount_spent DESC, cpm.company_domain_key, cpm.product_service
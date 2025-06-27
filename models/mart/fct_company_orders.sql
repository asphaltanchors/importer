/*
  Company-Level Order Analytics
  
  Aggregates order and line item data to the company level to enable
  "who buys what" analysis. Shows what products/services each consolidated
  company purchases, with quantities, frequencies, and trends.
*/

{{ config(
    materialized = 'table',
    tags = ['companies', 'orders', 'analytics', 'quickbooks']
) }}

WITH company_order_details AS (
    SELECT 
        -- Link to companies via customer mapping - use clean line items data
        bc.company_domain_key,
        
        -- Order information from clean line items table
        oli.order_number,
        oli.source_type as order_type,
        oli.order_date,
        oli.customer as customer_name,
        oli.status,
        
        -- Product/service details (already cleaned)
        oli.product_service,
        oli.product_service_description,
        oli.product_service_quantity as quantity,
        oli.product_service_rate as unit_price,
        oli.product_service_amount as line_amount,
        
        -- Order totals - we'll calculate these from line items since fct_order_line_items doesn't have order totals
        NULL::NUMERIC as order_total,
        NULL::NUMERIC as order_tax,
        
        -- Additional context
        oli.sales_rep,
        oli.payment_method,
        oli.terms
        
    FROM {{ ref('fct_order_line_items') }} oli
    INNER JOIN {{ ref('bridge_customer_company') }} bc 
        ON oli.customer = bc.customer_name
    WHERE oli.product_service_amount IS NOT NULL 
      AND oli.product_service_amount > 0
),

-- Enrich with product data
enriched_orders AS (
    SELECT 
        cod.*,
        -- Product enrichment from fct_products
        p.product_family,
        p.material_type,
        p.is_kit,
        p.item_type,
        p.sales_price as standard_sales_price,
        p.purchase_cost as standard_purchase_cost,
        
        -- Calculate margins when we have cost data
        CASE 
            WHEN p.purchase_cost > 0 AND cod.unit_price > 0 
            THEN ROUND(CAST((cod.unit_price - p.purchase_cost) * 100.0 / cod.unit_price AS NUMERIC), 2)
            ELSE NULL
        END as margin_percentage,
        
        -- Time-based categorization
        EXTRACT(YEAR FROM cod.order_date) as order_year,
        EXTRACT(MONTH FROM cod.order_date) as order_month,
        EXTRACT(QUARTER FROM cod.order_date) as order_quarter,
        
        -- Recency flags
        CASE 
            WHEN cod.order_date >= CURRENT_DATE - INTERVAL '90 days' THEN 'Recent'
            WHEN cod.order_date >= CURRENT_DATE - INTERVAL '1 year' THEN 'Last Year'
            WHEN cod.order_date >= CURRENT_DATE - INTERVAL '2 years' THEN 'Historical'
            ELSE 'Old'
        END as recency_category
        
    FROM company_order_details cod
    LEFT JOIN {{ ref('fct_products') }} p ON cod.product_service = p.item_name
    -- Only include orders that can be linked to companies
    WHERE cod.company_domain_key != 'NO_EMAIL_DOMAIN'
),

-- Create company-level order aggregations
company_order_summary AS (
    SELECT 
        company_domain_key,
        order_number,
        order_type,
        order_date,
        customer_name,
        sales_rep,
        payment_method,
        terms,
        
        -- Order-level aggregations
        COUNT(*) as line_item_count,
        SUM(line_amount) as calculated_order_total,
        MAX(order_total) as reported_order_total,  -- Use MAX since it's order-level
        MAX(order_tax) as order_tax,
        
        -- Product mix for this order
        COUNT(DISTINCT product_service) as unique_products,
        STRING_AGG(DISTINCT product_family, ', ') as product_families,
        STRING_AGG(DISTINCT material_type, ', ') as material_types,
        
        -- Order characteristics
        SUM(CASE WHEN is_kit THEN quantity ELSE 0 END) as kit_quantity,
        SUM(CASE WHEN is_kit THEN line_amount ELSE 0 END) as kit_amount,
        AVG(margin_percentage) as avg_margin_percentage,
        
        -- Time categorization
        order_year,
        order_quarter,
        recency_category
        
    FROM enriched_orders
    GROUP BY 
        company_domain_key, order_number, order_type, order_date, customer_name,
        sales_rep, payment_method, terms, order_year, order_quarter, recency_category
)

SELECT 
    -- Company and order identification
    cos.company_domain_key,
    fc.company_name,
    fc.domain_type,
    fc.business_size_category,
    cos.order_number,
    cos.order_type,
    cos.order_date,
    cos.customer_name,
    
    -- Order details
    cos.line_item_count,
    cos.calculated_order_total,
    cos.reported_order_total,
    cos.order_tax,
    cos.unique_products,
    cos.product_families,
    cos.material_types,
    
    -- Order characteristics
    cos.kit_quantity,
    cos.kit_amount,
    cos.avg_margin_percentage,
    
    -- Sales context
    cos.sales_rep,
    cos.payment_method,
    cos.terms,
    
    -- Time dimensions
    cos.order_year,
    cos.order_quarter,
    cos.recency_category,
    
    -- Order size classification
    CASE 
        WHEN cos.calculated_order_total >= 10000 THEN 'Large Order ($10K+)'
        WHEN cos.calculated_order_total >= 2500 THEN 'Medium Order ($2.5K-$10K)'
        WHEN cos.calculated_order_total >= 500 THEN 'Small Order ($500-$2.5K)'
        ELSE 'Micro Order (<$500)'
    END as order_size_category,
    
    -- Product diversity
    CASE 
        WHEN cos.unique_products >= 10 THEN 'High Diversity (10+ products)'
        WHEN cos.unique_products >= 5 THEN 'Medium Diversity (5-9 products)'
        WHEN cos.unique_products >= 2 THEN 'Low Diversity (2-4 products)'
        ELSE 'Single Product'
    END as product_diversity,
    
    -- Days since order
    CURRENT_DATE - cos.order_date as days_since_order,
    
    -- Metadata
    CURRENT_TIMESTAMP as created_at
    
FROM company_order_summary cos
INNER JOIN {{ ref('fct_companies') }} fc ON cos.company_domain_key = fc.company_domain_key
ORDER BY cos.company_domain_key, cos.order_date DESC
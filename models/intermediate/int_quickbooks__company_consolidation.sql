-- ABOUTME: Intermediate model consolidating customers into companies based on domain mapping
-- ABOUTME: Handles domain-based company consolidation with representative selection and revenue aggregation

{{ config(
    materialized = 'table',
    tags = ['intermediate', 'companies', 'consolidation']
) }}

-- Get revenue data by domain from customer mapping and revenue models
WITH domain_revenue AS (
    SELECT 
        ccm.company_domain_key as normalized_domain,
        SUM(cr.customer_total_revenue) as total_revenue,
        SUM(cr.customer_total_orders) as total_orders,
        SUM(cr.customer_total_line_items) as total_line_items,
        MIN(cr.customer_first_order_date) as first_order_date,
        MAX(cr.customer_latest_order_date) as latest_order_date
    FROM {{ ref('int_quickbooks__customer_company_mapping') }} ccm
    LEFT JOIN {{ ref('int_quickbooks__customer_revenue') }} cr 
        ON ccm.customer_name = cr.customer
    WHERE ccm.company_domain_key != 'NO_EMAIL_DOMAIN'
    GROUP BY ccm.company_domain_key
),

-- Select best representative for each domain using customer mapping
domain_representatives AS (
    SELECT DISTINCT
        ccm.company_domain_key as normalized_domain,
        ccm.domain_type,
        FIRST_VALUE(
            CASE 
                WHEN ccm.customer_company_name IS NOT NULL AND TRIM(ccm.customer_company_name) != '' 
                THEN ccm.customer_company_name
                ELSE ccm.customer_name
            END
        ) OVER (
            PARTITION BY ccm.company_domain_key 
            ORDER BY 
                LENGTH(COALESCE(ccm.customer_company_name, '')) DESC,
                ccm.current_balance DESC NULLS LAST,
                ccm.customer_id
        ) as primary_company_name,
        
        FIRST_VALUE(ccm.main_email) OVER (
            PARTITION BY ccm.company_domain_key 
            ORDER BY 
                ccm.current_balance DESC NULLS LAST,
                ccm.customer_id
        ) as primary_email,
        
        FIRST_VALUE(ccm.main_phone) OVER (
            PARTITION BY ccm.company_domain_key 
            ORDER BY 
                ccm.current_balance DESC NULLS LAST,
                ccm.customer_id
        ) as primary_phone,
        
        FIRST_VALUE(ccm.billing_address_line_1) OVER (
            PARTITION BY ccm.company_domain_key 
            ORDER BY 
                ccm.current_balance DESC NULLS LAST,
                ccm.customer_id
        ) as primary_billing_address_line_1,
        
        FIRST_VALUE(ccm.billing_address_city) OVER (
            PARTITION BY ccm.company_domain_key 
            ORDER BY 
                ccm.current_balance DESC NULLS LAST,
                ccm.customer_id
        ) as primary_billing_city,
        
        FIRST_VALUE(ccm.billing_address_state) OVER (
            PARTITION BY ccm.company_domain_key 
            ORDER BY 
                ccm.current_balance DESC NULLS LAST,
                ccm.customer_id
        ) as primary_billing_state,
        
        FIRST_VALUE(ccm.billing_address_postal_code) OVER (
            PARTITION BY ccm.company_domain_key 
            ORDER BY 
                ccm.current_balance DESC NULLS LAST,
                ccm.customer_id
        ) as primary_billing_postal_code
        
    FROM {{ ref('int_quickbooks__customer_company_mapping') }} ccm
    WHERE ccm.domain_type IN ('corporate', 'individual')
      AND ccm.company_domain_key != 'NO_EMAIL_DOMAIN'
),

-- Aggregate customer metrics by domain
company_aggregates AS (
    SELECT 
        ccm.company_domain_key as normalized_domain,
        ccm.domain_type,
        
        -- Aggregated metrics
        COUNT(*) as customer_count,
        COUNT(DISTINCT ccm.customer_name) as unique_customer_names,
        COUNT(DISTINCT ccm.customer_company_name) as unique_company_names,
        SUM(ccm.current_balance) as total_current_balance,
        
        -- All customer names for reference (limited to avoid huge strings)
        STRING_AGG(
            DISTINCT CASE 
                WHEN ccm.customer_company_name IS NOT NULL AND TRIM(ccm.customer_company_name) != '' 
                THEN ccm.customer_company_name
                ELSE ccm.customer_name
            END, 
            ' | '
        ) as all_customer_names
        
    FROM {{ ref('int_quickbooks__customer_company_mapping') }} ccm
    -- Only include corporate domains or individuals with valid domains
    WHERE ccm.domain_type IN ('corporate', 'individual')
      AND ccm.company_domain_key != 'NO_EMAIL_DOMAIN'
    GROUP BY ccm.company_domain_key, ccm.domain_type
),

-- Get geographic data from customer addresses (simplified for XLSX migration)
company_geographic_data AS (
    SELECT 
        company_domain_key as normalized_domain,
        -- Use most common state/country from customer addresses
        MODE() WITHIN GROUP (ORDER BY billing_address_state) as primary_country,
        MODE() WITHIN GROUP (ORDER BY billing_address_state) as region,
        'Unknown' as country_category
    FROM {{ ref('int_quickbooks__customer_company_mapping') }}
    WHERE billing_address_state IS NOT NULL
      AND company_domain_key != 'NO_EMAIL_DOMAIN'
    GROUP BY company_domain_key
)

-- Combine all consolidation logic
SELECT 
    ca.normalized_domain as company_domain_key,
    ca.domain_type,
    dr.primary_company_name as company_name,
    dr.primary_email,
    dr.primary_phone,
    dr.primary_billing_address_line_1,
    dr.primary_billing_city,
    dr.primary_billing_state,
    dr.primary_billing_postal_code,
    
    -- Geographic data from orders
    geo.primary_country,
    geo.region,
    geo.country_category,
    
    -- Customer metrics
    COALESCE(ca.customer_count, 0) as customer_count,
    COALESCE(ca.unique_customer_names, 0) as unique_customer_names,
    COALESCE(ca.unique_company_names, 0) as unique_company_names,
    COALESCE(ca.total_current_balance, 0) as total_current_balance,
    
    -- Revenue metrics
    COALESCE(rev.total_revenue, 0) as total_revenue,
    COALESCE(rev.total_orders, 0) as total_orders,
    COALESCE(rev.total_line_items, 0) as total_line_items,
    rev.first_order_date,
    rev.latest_order_date,
    
    -- Business classification
    CASE 
        WHEN ca.domain_type = 'individual' THEN 'Individual Customer'
        WHEN ca.customer_count = 1 THEN 'Single Location'
        WHEN ca.customer_count BETWEEN 2 AND 5 THEN 'Small Multi-Location'
        WHEN ca.customer_count BETWEEN 6 AND 20 THEN 'Medium Multi-Location'
        ELSE 'Large Multi-Location'
    END as business_size_category,
    
    -- Revenue classification
    CASE 
        WHEN COALESCE(rev.total_revenue, 0) >= 100000 THEN 'High Value ($100K+)'
        WHEN COALESCE(rev.total_revenue, 0) >= 25000 THEN 'Medium Value ($25K-$100K)'
        WHEN COALESCE(rev.total_revenue, 0) >= 5000 THEN 'Growing Value ($5K-$25K)'
        WHEN COALESCE(rev.total_revenue, 0) > 0 THEN 'Low Value (<$5K)'
        ELSE 'No Revenue'
    END as revenue_category,
    
    -- Limited customer names sample (first 500 chars to avoid huge fields)
    LEFT(ca.all_customer_names, 500) as customer_names_sample,
    
    -- Activity flags
    CASE WHEN COALESCE(rev.total_revenue, 0) > 0 THEN TRUE ELSE FALSE END as has_revenue,
    CASE WHEN ca.customer_count > 1 THEN TRUE ELSE FALSE END as is_multi_location,
    CASE WHEN ca.domain_type = 'corporate' THEN TRUE ELSE FALSE END as is_corporate,
    
    -- Metadata
    CURRENT_TIMESTAMP as created_at
    
FROM company_aggregates ca
INNER JOIN domain_representatives dr ON ca.normalized_domain = dr.normalized_domain
LEFT JOIN domain_revenue rev ON ca.normalized_domain = rev.normalized_domain
LEFT JOIN company_geographic_data geo ON ca.normalized_domain = geo.normalized_domain

-- Focus on companies with meaningful presence
WHERE COALESCE(rev.total_revenue, 0) > 0 
   OR ca.customer_count > 1
   OR ca.domain_type = 'corporate'
ORDER BY COALESCE(rev.total_revenue, 0) DESC, ca.customer_count DESC
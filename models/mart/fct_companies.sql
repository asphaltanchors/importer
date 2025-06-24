/*
  Company Master Fact Table
  
  Creates consolidated companies based on email domain consolidation.
  One row per company with aggregated metrics and clean company profiles.
  
  Uses the domain mapping created by domain_consolidation.py to consolidate
  customers with similar email domains into unified company records.
*/

{{ config(
    materialized = 'table',
    tags = ['companies', 'quickbooks', 'consolidation']
) }}

WITH customers_with_domains AS (
    SELECT 
        c.*,
        -- Extract primary email domain
        CASE 
            WHEN c.main_email LIKE '%;%' THEN LOWER(SPLIT_PART(TRIM(SPLIT_PART(c.main_email, ';', 1)), '@', 2))
            ELSE LOWER(SPLIT_PART(TRIM(c.main_email), '@', 2))
        END as main_email_domain,
        -- Extract cc email domain  
        CASE 
            WHEN c.cc_email LIKE '%;%' THEN LOWER(SPLIT_PART(TRIM(SPLIT_PART(c.cc_email, ';', 1)), '@', 2))
            ELSE LOWER(SPLIT_PART(TRIM(c.cc_email), '@', 2))
        END as cc_email_domain
    FROM {{ source('raw_data', 'customers') }} c
),

customers_with_normalized_domains AS (
    SELECT 
        c.*,
        -- Get normalized domain (prefer main_email, fallback to cc_email)
        COALESCE(
            dm_main.normalized_domain,
            dm_cc.normalized_domain,
            'NO_EMAIL_DOMAIN'
        ) as normalized_domain,
        COALESCE(
            dm_main.domain_type,
            dm_cc.domain_type,
            'no_email'
        ) as domain_type
    FROM customers_with_domains c
    LEFT JOIN {{ source('raw_data', 'domain_mapping') }} dm_main 
        ON c.main_email_domain = dm_main.original_domain
    LEFT JOIN {{ source('raw_data', 'domain_mapping') }} dm_cc 
        ON c.cc_email_domain = dm_cc.original_domain
),

-- Get revenue data by mapping customers to domains
invoice_revenue_by_domain AS (
    SELECT 
        COALESCE(
            dm_main.normalized_domain,
            dm_cc.normalized_domain,
            'NO_EMAIL_DOMAIN'
        ) as normalized_domain,
        SUM(CAST(NULLIF(TRIM(i.product_service_amount), '') AS NUMERIC)) as total_revenue,
        COUNT(DISTINCT i.invoice_no) as total_invoices,
        COUNT(*) as total_line_items,
        MIN(CAST(i.invoice_date AS DATE)) as first_order_date,
        MAX(CAST(i.invoice_date AS DATE)) as latest_order_date
    FROM {{ source('raw_data', 'invoices') }} i
    LEFT JOIN {{ source('raw_data', 'customers') }} c ON i.customer = c.customer_name
    LEFT JOIN {{ source('raw_data', 'domain_mapping') }} dm_main 
        ON LOWER(SPLIT_PART(TRIM(SPLIT_PART(COALESCE(c.main_email, ''), ';', 1)), '@', 2)) = dm_main.original_domain
    LEFT JOIN {{ source('raw_data', 'domain_mapping') }} dm_cc 
        ON LOWER(SPLIT_PART(TRIM(SPLIT_PART(COALESCE(c.cc_email, ''), ';', 1)), '@', 2)) = dm_cc.original_domain
    WHERE i.product_service_amount IS NOT NULL 
      AND TRIM(i.product_service_amount) ~ '^[0-9]+(\.[0-9]+)?$'
      AND CAST(NULLIF(TRIM(i.product_service_amount), '') AS NUMERIC) > 0
    GROUP BY COALESCE(dm_main.normalized_domain, dm_cc.normalized_domain, 'NO_EMAIL_DOMAIN')
),

sales_revenue_by_domain AS (
    SELECT 
        COALESCE(
            dm_main.normalized_domain,
            dm_cc.normalized_domain,
            'NO_EMAIL_DOMAIN'
        ) as normalized_domain,
        SUM(CAST(NULLIF(TRIM(s.product_service_amount), '') AS NUMERIC)) as total_revenue,
        COUNT(DISTINCT s.sales_receipt_no) as total_invoices,
        COUNT(*) as total_line_items,
        MIN(CAST(s.sales_receipt_date AS DATE)) as first_order_date,
        MAX(CAST(s.sales_receipt_date AS DATE)) as latest_order_date
    FROM {{ source('raw_data', 'sales_receipts') }} s
    LEFT JOIN {{ source('raw_data', 'customers') }} c ON s.customer = c.customer_name
    LEFT JOIN {{ source('raw_data', 'domain_mapping') }} dm_main 
        ON LOWER(SPLIT_PART(TRIM(SPLIT_PART(COALESCE(c.main_email, ''), ';', 1)), '@', 2)) = dm_main.original_domain
    LEFT JOIN {{ source('raw_data', 'domain_mapping') }} dm_cc 
        ON LOWER(SPLIT_PART(TRIM(SPLIT_PART(COALESCE(c.cc_email, ''), ';', 1)), '@', 2)) = dm_cc.original_domain
    WHERE s.product_service_amount IS NOT NULL 
      AND TRIM(s.product_service_amount) ~ '^[0-9]+(\.[0-9]+)?$'
      AND CAST(NULLIF(TRIM(s.product_service_amount), '') AS NUMERIC) > 0
    GROUP BY COALESCE(dm_main.normalized_domain, dm_cc.normalized_domain, 'NO_EMAIL_DOMAIN')
),

aggregated_revenue AS (
    SELECT 
        normalized_domain,
        SUM(total_revenue) as total_revenue,
        SUM(total_invoices) as total_invoices,
        SUM(total_line_items) as total_line_items,
        MIN(first_order_date) as first_order_date,
        MAX(latest_order_date) as latest_order_date
    FROM (
        SELECT * FROM invoice_revenue_by_domain
        UNION ALL
        SELECT * FROM sales_revenue_by_domain
    ) combined_revenue
    WHERE normalized_domain != 'NO_EMAIL_DOMAIN'
    GROUP BY normalized_domain
),

-- First, get the "best" representative for each domain
domain_representatives AS (
    SELECT DISTINCT
        c.normalized_domain,
        c.domain_type,
        FIRST_VALUE(
            CASE 
                WHEN c.company_name IS NOT NULL AND TRIM(c.company_name) != '' 
                THEN c.company_name
                ELSE c.customer_name
            END
        ) OVER (
            PARTITION BY c.normalized_domain 
            ORDER BY 
                LENGTH(COALESCE(c.company_name, '')) DESC,
                CAST(NULLIF(TRIM(c.current_balance), '') AS NUMERIC) DESC NULLS LAST,
                c.quick_books_internal_id
        ) as primary_company_name,
        
        FIRST_VALUE(c.main_email) OVER (
            PARTITION BY c.normalized_domain 
            ORDER BY 
                CAST(NULLIF(TRIM(c.current_balance), '') AS NUMERIC) DESC NULLS LAST,
                c.quick_books_internal_id
        ) as primary_email,
        
        FIRST_VALUE(c.main_phone) OVER (
            PARTITION BY c.normalized_domain 
            ORDER BY 
                CAST(NULLIF(TRIM(c.current_balance), '') AS NUMERIC) DESC NULLS LAST,
                c.quick_books_internal_id
        ) as primary_phone,
        
        FIRST_VALUE(c.billing_address_line_1) OVER (
            PARTITION BY c.normalized_domain 
            ORDER BY 
                CAST(NULLIF(TRIM(c.current_balance), '') AS NUMERIC) DESC NULLS LAST,
                c.quick_books_internal_id
        ) as primary_billing_address_line_1,
        
        FIRST_VALUE(c.billing_address_city) OVER (
            PARTITION BY c.normalized_domain 
            ORDER BY 
                CAST(NULLIF(TRIM(c.current_balance), '') AS NUMERIC) DESC NULLS LAST,
                c.quick_books_internal_id
        ) as primary_billing_city,
        
        FIRST_VALUE(c.billing_address_state) OVER (
            PARTITION BY c.normalized_domain 
            ORDER BY 
                CAST(NULLIF(TRIM(c.current_balance), '') AS NUMERIC) DESC NULLS LAST,
                c.quick_books_internal_id
        ) as primary_billing_state,
        
        FIRST_VALUE(c.billing_address_postal_code) OVER (
            PARTITION BY c.normalized_domain 
            ORDER BY 
                CAST(NULLIF(TRIM(c.current_balance), '') AS NUMERIC) DESC NULLS LAST,
                c.quick_books_internal_id
        ) as primary_billing_postal_code
        
    FROM customers_with_normalized_domains c
    WHERE c.domain_type IN ('corporate', 'individual')
      AND c.normalized_domain != 'NO_EMAIL_DOMAIN'
),

-- Aggregate customers by normalized domain
company_aggregates AS (
    SELECT 
        c.normalized_domain,
        c.domain_type,
        
        -- Aggregated metrics
        COUNT(*) as customer_count,
        COUNT(DISTINCT c.customer_name) as unique_customer_names,
        COUNT(DISTINCT c.company_name) as unique_company_names,
        SUM(CAST(NULLIF(TRIM(c.current_balance), '') AS NUMERIC)) as total_current_balance,
        
        -- All customer names for reference (limited to avoid huge strings)
        STRING_AGG(
            DISTINCT CASE 
                WHEN c.company_name IS NOT NULL AND TRIM(c.company_name) != '' 
                THEN c.company_name
                ELSE c.customer_name
            END, 
            ' | '
        ) as all_customer_names
        
    FROM customers_with_normalized_domains c
    -- Only include corporate domains or individuals with revenue
    WHERE c.domain_type IN ('corporate', 'individual')
      AND c.normalized_domain != 'NO_EMAIL_DOMAIN'
    GROUP BY c.normalized_domain, c.domain_type
),

-- Get geographic data by aggregating from orders
company_geographic_data AS (
    SELECT 
        COALESCE(
            dm_main.normalized_domain,
            dm_cc.normalized_domain,
            'NO_EMAIL_DOMAIN'
        ) as normalized_domain,
        -- Use MODE() to get the most common country/region per company
        MODE() WITHIN GROUP (ORDER BY o.primary_country) as primary_country,
        MODE() WITHIN GROUP (ORDER BY o.region) as region,
        MODE() WITHIN GROUP (ORDER BY o.country_category) as country_category
    FROM {{ ref('fct_orders') }} o
    LEFT JOIN {{ source('raw_data', 'customers') }} c ON o.customer = c.customer_name
    LEFT JOIN {{ source('raw_data', 'domain_mapping') }} dm_main 
        ON LOWER(SPLIT_PART(TRIM(SPLIT_PART(COALESCE(c.main_email, ''), ';', 1)), '@', 2)) = dm_main.original_domain
    LEFT JOIN {{ source('raw_data', 'domain_mapping') }} dm_cc 
        ON LOWER(SPLIT_PART(TRIM(SPLIT_PART(COALESCE(c.cc_email, ''), ';', 1)), '@', 2)) = dm_cc.original_domain
    WHERE o.primary_country IS NOT NULL
      AND COALESCE(dm_main.normalized_domain, dm_cc.normalized_domain, 'NO_EMAIL_DOMAIN') != 'NO_EMAIL_DOMAIN'
    GROUP BY COALESCE(dm_main.normalized_domain, dm_cc.normalized_domain, 'NO_EMAIL_DOMAIN')
),

-- Join with revenue data and geographic data
company_facts AS (
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
        ca.customer_count,
        ca.unique_customer_names,
        ca.unique_company_names,
        ca.total_current_balance,
        
        -- Revenue metrics (sum across all customers for this domain)
        COALESCE(SUM(rev.total_revenue), 0) as total_revenue,
        COALESCE(SUM(rev.total_invoices), 0) as total_orders,
        COALESCE(SUM(rev.total_line_items), 0) as total_line_items,
        MIN(rev.first_order_date) as first_order_date,
        MAX(rev.latest_order_date) as latest_order_date,
        
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
            WHEN COALESCE(SUM(rev.total_revenue), 0) >= 100000 THEN 'High Value ($100K+)'
            WHEN COALESCE(SUM(rev.total_revenue), 0) >= 25000 THEN 'Medium Value ($25K-$100K)'
            WHEN COALESCE(SUM(rev.total_revenue), 0) >= 5000 THEN 'Growing Value ($5K-$25K)'
            WHEN COALESCE(SUM(rev.total_revenue), 0) > 0 THEN 'Low Value (<$5K)'
            ELSE 'No Revenue'
        END as revenue_category,
        
        -- Limited customer names sample (first 500 chars to avoid huge fields)
        LEFT(ca.all_customer_names, 500) as customer_names_sample,
        
        -- Metadata
        CURRENT_TIMESTAMP as created_at
        
    FROM company_aggregates ca
    INNER JOIN domain_representatives dr ON ca.normalized_domain = dr.normalized_domain
    LEFT JOIN aggregated_revenue rev ON ca.normalized_domain = rev.normalized_domain
    LEFT JOIN company_geographic_data geo ON ca.normalized_domain = geo.normalized_domain
    GROUP BY 
        ca.normalized_domain, ca.domain_type, dr.primary_company_name,
        dr.primary_email, dr.primary_phone, 
        dr.primary_billing_address_line_1, dr.primary_billing_city, 
        dr.primary_billing_state, dr.primary_billing_postal_code,
        geo.primary_country, geo.region, geo.country_category,
        ca.customer_count, ca.unique_customer_names, ca.unique_company_names,
        ca.total_current_balance, ca.all_customer_names
)

SELECT * 
FROM company_facts
-- Focus on companies with meaningful presence (revenue or multiple customers)
WHERE total_revenue > 0 
   OR customer_count > 1
   OR domain_type = 'corporate'
ORDER BY total_revenue DESC, customer_count DESC
/*
  Customer-Company Bridge Table
  
  Links individual QuickBooks customers to consolidated companies,
  enabling drill-down analysis from company level to customer level.
  
  This bridge table preserves the relationship between:
  - Consolidated companies (fct_companies)
  - Individual QuickBooks customers (raw customers)
  - Revenue attribution and customer details
*/

{{ config(
    materialized = 'table',
    tags = ['bridge', 'companies', 'customers', 'quickbooks']
) }}

WITH customers_with_domains AS (
    SELECT 
        c.quick_books_internal_id,
        c.customer_name,
        c.company_name,
        c.main_email,
        c.cc_email,
        c.main_phone,
        c.billing_address_line_1,
        c.billing_address_city,
        c.billing_address_state,
        c.billing_address_postal_code,
        c.sales_rep,
        c.terms,
        c.price_level,
        CAST(NULLIF(TRIM(c.current_balance), '') AS NUMERIC) as current_balance,
        c.status as customer_status,
        CAST(c.created_date AS TIMESTAMP) as customer_created_date,
        CAST(c.modified_date AS TIMESTAMP) as customer_modified_date,
        
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
        ) as company_domain_key,
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

-- Get customer-level revenue from orders
customer_revenue AS (
    SELECT 
        customer,
        SUM(CAST(NULLIF(TRIM(product_service_amount), '') AS NUMERIC)) as customer_total_revenue,
        COUNT(DISTINCT invoice_no) as customer_total_orders,
        COUNT(*) as customer_total_line_items,
        MIN(CAST(invoice_date AS DATE)) as customer_first_order_date,
        MAX(CAST(invoice_date AS DATE)) as customer_latest_order_date,
        COUNT(DISTINCT CAST(invoice_date AS DATE)) as customer_order_days
    FROM {{ source('raw_data', 'invoices') }}
    WHERE product_service_amount IS NOT NULL 
      AND TRIM(product_service_amount) ~ '^[0-9]+(\.[0-9]+)?$'
      AND CAST(NULLIF(TRIM(product_service_amount), '') AS NUMERIC) > 0
    GROUP BY customer
    
    UNION ALL
    
    SELECT 
        customer,
        SUM(CAST(NULLIF(TRIM(product_service_amount), '') AS NUMERIC)) as customer_total_revenue,
        COUNT(DISTINCT sales_receipt_no) as customer_total_orders,
        COUNT(*) as customer_total_line_items,
        MIN(CAST(sales_receipt_date AS DATE)) as customer_first_order_date,
        MAX(CAST(sales_receipt_date AS DATE)) as customer_latest_order_date,
        COUNT(DISTINCT CAST(sales_receipt_date AS DATE)) as customer_order_days
    FROM {{ source('raw_data', 'sales_receipts') }}
    WHERE product_service_amount IS NOT NULL 
      AND TRIM(product_service_amount) ~ '^[0-9]+(\.[0-9]+)?$'
      AND CAST(NULLIF(TRIM(product_service_amount), '') AS NUMERIC) > 0
    GROUP BY customer
),

aggregated_customer_revenue AS (
    SELECT 
        customer,
        SUM(customer_total_revenue) as customer_total_revenue,
        SUM(customer_total_orders) as customer_total_orders,
        SUM(customer_total_line_items) as customer_total_line_items,
        MIN(customer_first_order_date) as customer_first_order_date,
        MAX(customer_latest_order_date) as customer_latest_order_date,
        SUM(customer_order_days) as customer_order_days
    FROM customer_revenue
    GROUP BY customer
),

-- Create the bridge with customer details and revenue
customer_company_bridge AS (
    SELECT 
        -- Bridge keys
        c.quick_books_internal_id as customer_id,
        c.company_domain_key,
        
        -- Customer details
        c.customer_name,
        c.company_name as customer_company_name,
        c.main_email,
        c.cc_email,
        c.main_phone,
        c.billing_address_line_1,
        c.billing_address_city,
        c.billing_address_state,
        c.billing_address_postal_code,
        c.sales_rep,
        c.terms,
        c.price_level,
        c.current_balance,
        c.customer_status,
        c.customer_created_date,
        c.customer_modified_date,
        
        -- Domain classification
        c.domain_type,
        
        -- Customer revenue metrics
        COALESCE(rev.customer_total_revenue, 0) as customer_total_revenue,
        COALESCE(rev.customer_total_orders, 0) as customer_total_orders,
        COALESCE(rev.customer_total_line_items, 0) as customer_total_line_items,
        rev.customer_first_order_date,
        rev.customer_latest_order_date,
        COALESCE(rev.customer_order_days, 0) as customer_order_days,
        
        -- Customer value classification
        CASE 
            WHEN COALESCE(rev.customer_total_revenue, 0) >= 50000 THEN 'High Value Customer ($50K+)'
            WHEN COALESCE(rev.customer_total_revenue, 0) >= 10000 THEN 'Medium Value Customer ($10K-$50K)'
            WHEN COALESCE(rev.customer_total_revenue, 0) >= 1000 THEN 'Regular Customer ($1K-$10K)'
            WHEN COALESCE(rev.customer_total_revenue, 0) > 0 THEN 'Low Value Customer (<$1K)'
            ELSE 'No Revenue'
        END as customer_value_tier,
        
        -- Customer activity classification
        CASE 
            WHEN rev.customer_latest_order_date >= CURRENT_DATE - INTERVAL '90 days' THEN 'Active (Last 90 Days)'
            WHEN rev.customer_latest_order_date >= CURRENT_DATE - INTERVAL '1 year' THEN 'Recent (Last Year)'
            WHEN rev.customer_latest_order_date >= CURRENT_DATE - INTERVAL '2 years' THEN 'Dormant (1-2 Years)'
            WHEN rev.customer_latest_order_date IS NOT NULL THEN 'Inactive (2+ Years)'
            ELSE 'No Orders'
        END as customer_activity_status,
        
        -- Customer ordering frequency
        CASE 
            WHEN rev.customer_order_days > 0 AND rev.customer_total_orders > 0 THEN
                ROUND(CAST(rev.customer_total_orders AS NUMERIC) / CAST(rev.customer_order_days AS NUMERIC), 2)
            ELSE 0
        END as orders_per_day,
        
        -- Flags for analysis
        CASE WHEN c.domain_type = 'individual' THEN TRUE ELSE FALSE END as is_individual_customer,
        CASE WHEN c.company_domain_key = 'NO_EMAIL_DOMAIN' THEN TRUE ELSE FALSE END as is_missing_email,
        CASE WHEN COALESCE(rev.customer_total_revenue, 0) > 0 THEN TRUE ELSE FALSE END as has_revenue,
        
        -- Metadata
        CURRENT_TIMESTAMP as created_at
        
    FROM customers_with_normalized_domains c
    LEFT JOIN aggregated_customer_revenue rev ON c.customer_name = rev.customer
    -- Only include customers that belong to consolidated companies or have revenue
    WHERE (c.company_domain_key != 'NO_EMAIL_DOMAIN' AND c.domain_type IN ('corporate', 'individual'))
       OR COALESCE(rev.customer_total_revenue, 0) > 0
)

SELECT * 
FROM customer_company_bridge
ORDER BY customer_total_revenue DESC, customer_name
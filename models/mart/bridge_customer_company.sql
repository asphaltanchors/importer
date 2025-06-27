/*
  Customer-Company Bridge Table
  
  Links individual QuickBooks customers to consolidated companies,
  enabling drill-down analysis from company level to customer level.
  
  Uses intermediate models for proper DBT layering:
  - int_quickbooks__customer_company_mapping for customer-domain mapping
  - int_quickbooks__customer_revenue for revenue calculations
*/

{{ config(
    materialized = 'table',
    tags = ['bridge', 'companies', 'customers', 'quickbooks']
) }}

SELECT 
    -- Bridge keys
    ccm.customer_id,
    ccm.company_domain_key,
    
    -- Customer details
    ccm.customer_name,
    ccm.customer_company_name,
    ccm.standardized_customer_name,
    ccm.customer_name_normalization_type,
    ccm.main_email,
    ccm.cc_email,
    ccm.main_phone,
    ccm.billing_address_line_1,
    ccm.billing_address_city,
    ccm.billing_address_state,
    ccm.billing_address_postal_code,
    ccm.sales_rep,
    ccm.terms,
    ccm.price_level,
    ccm.current_balance,
    ccm.customer_status,
    ccm.customer_created_date,
    ccm.customer_modified_date,
    
    -- Domain classification
    ccm.domain_type,
    
    -- Customer revenue metrics
    COALESCE(cr.customer_total_revenue, 0) as customer_total_revenue,
    COALESCE(cr.customer_total_orders, 0) as customer_total_orders,
    COALESCE(cr.customer_total_line_items, 0) as customer_total_line_items,
    cr.customer_first_order_date,
    cr.customer_latest_order_date,
    COALESCE(cr.customer_order_days, 0) as customer_order_days,
    
    -- Customer classifications from revenue model
    cr.customer_value_tier,
    cr.customer_activity_status,
    cr.orders_per_day,
    
    -- Flags for analysis
    ccm.is_individual_customer,
    ccm.is_missing_email,
    COALESCE(cr.has_revenue, FALSE) as has_revenue,
    COALESCE(cr.is_active_customer, FALSE) as is_active_customer,
    
    -- Metadata
    CURRENT_TIMESTAMP as created_at
    
FROM {{ ref('int_quickbooks__customer_company_mapping') }} ccm
LEFT JOIN {{ ref('int_quickbooks__customer_revenue') }} cr 
    ON ccm.customer_name = cr.customer
-- Only include customers that belong to consolidated companies or have revenue
WHERE (ccm.company_domain_key != 'NO_EMAIL_DOMAIN' AND ccm.domain_type IN ('corporate', 'individual'))
   OR COALESCE(cr.customer_total_revenue, 0) > 0

ORDER BY COALESCE(cr.customer_total_revenue, 0) DESC, ccm.customer_name
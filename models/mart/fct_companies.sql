/*
  Company Master Fact Table
  
  Creates consolidated companies based on email domain consolidation.
  One row per company with aggregated metrics and clean company profiles.
  
  Uses intermediate models for proper DBT layering:
  - int_quickbooks__company_consolidation for domain-based consolidation
  - stg_quickbooks__company_enrichment for external API data
*/

{{ config(
    materialized = 'table',
    tags = ['companies', 'quickbooks', 'consolidation']
) }}

SELECT 
    cc.company_domain_key,
    cc.domain_type,
    cc.company_name,
    cc.primary_email,
    cc.primary_phone,
    cc.primary_billing_address_line_1,
    cc.primary_billing_city,
    cc.primary_billing_state,
    cc.primary_billing_postal_code,
    
    -- Geographic data
    cc.primary_country,
    cc.region,
    cc.country_category,
    
    -- Customer metrics
    cc.customer_count,
    cc.unique_customer_names,
    cc.unique_company_names,
    cc.total_current_balance,
    
    -- Revenue metrics
    cc.total_revenue,
    cc.total_orders,
    cc.total_line_items,
    cc.first_order_date,
    cc.latest_order_date,
    
    -- Business classification
    cc.business_size_category,
    cc.revenue_category,
    cc.customer_names_sample,
    
    -- Activity flags
    cc.has_revenue,
    cc.is_multi_location,
    cc.is_corporate,
    
    -- Enrichment data from external APIs
    ce.enriched_industry,
    ce.enriched_employee_count,
    ce.enriched_description,
    ce.enriched_founded_year,
    ce.enriched_annual_revenue,
    ce.enrichment_source,
    ce.enrichment_date,
    
    -- Metadata
    CURRENT_TIMESTAMP as created_at
    
FROM {{ ref('int_quickbooks__company_consolidation') }} cc
LEFT JOIN {{ ref('stg_quickbooks__company_enrichment') }} ce 
    ON cc.company_domain_key = ce.company_domain

ORDER BY cc.total_revenue DESC, cc.customer_count DESC
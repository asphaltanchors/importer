-- ABOUTME: Staging model for company enrichment data from external APIs
-- ABOUTME: Returns empty result set when company_enrichment table doesn't exist

{{ config(materialized = 'view') }}

-- Return empty result set with expected schema when table doesn't exist
SELECT 
    NULL::VARCHAR as company_domain,
    NULL::VARCHAR as company_name,
    NULL::VARCHAR as enrichment_source,
    NULL::TIMESTAMP as enrichment_date,
    NULL::VARCHAR as load_date,
    NULL::BOOLEAN as is_manual_load,
    NULL::VARCHAR as enriched_industry,
    NULL::INTEGER as enriched_employee_count,
    NULL::VARCHAR as enriched_description,
    NULL::INTEGER as enriched_founded_year,
    NULL::VARCHAR as enriched_annual_revenue
WHERE FALSE  -- Always return empty result set
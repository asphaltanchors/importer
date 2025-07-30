-- ABOUTME: Staging model for email domain to company consolidation mapping
-- ABOUTME: Cleans and standardizes domain mapping data created by domain_consolidation.py

{{ config(
    materialized = 'table',
    tags = ['staging', 'domain_mapping']
) }}

SELECT 
    -- Domain mapping fields
    LOWER(TRIM(original_domain)) as original_domain,
    LOWER(TRIM(normalized_domain)) as normalized_domain,
    LOWER(TRIM(domain_type)) as domain_type,
    
    -- Metadata
    CAST(created_date AS TIMESTAMP) as mapping_created_date,
    CURRENT_TIMESTAMP as staged_at

FROM {{ source('raw_data', 'domain_mapping') }}

-- Data quality filters
WHERE original_domain IS NOT NULL 
  AND normalized_domain IS NOT NULL
  AND domain_type IS NOT NULL
  AND TRIM(original_domain) != ''
  AND TRIM(normalized_domain) != ''
  AND TRIM(domain_type) != ''

ORDER BY original_domain
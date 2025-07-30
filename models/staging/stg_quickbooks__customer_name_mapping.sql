-- ABOUTME: Staging model for customer name standardization mapping  
-- ABOUTME: Cleans and standardizes customer name mapping data created by domain_consolidation.py

{{ config(
    materialized = 'table',
    tags = ['staging', 'customer_name_mapping']
) }}

SELECT 
    -- Customer name mapping fields
    TRIM(original_name) as original_name,
    TRIM(normalized_name) as normalized_name,
    LOWER(TRIM(normalization_type)) as normalization_type,
    
    -- Metadata
    CAST(created_date AS TIMESTAMP) as mapping_created_date,
    CURRENT_TIMESTAMP as staged_at

FROM {{ source('raw_data', 'customer_name_mapping') }}

-- Data quality filters
WHERE original_name IS NOT NULL 
  AND normalized_name IS NOT NULL
  AND normalization_type IS NOT NULL
  AND TRIM(original_name) != ''
  AND TRIM(normalized_name) != ''
  AND TRIM(normalization_type) != ''

ORDER BY original_name
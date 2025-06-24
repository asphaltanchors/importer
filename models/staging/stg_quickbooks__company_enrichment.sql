-- ABOUTME: Staging model for company enrichment data from external APIs
-- ABOUTME: Extracts key fields from JSONB while preserving full raw data for future use

{{ config(materialized = 'view') }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'company_enrichment') }}
),

enrichment_parsed AS (
    SELECT 
        company_domain,
        company_name,
        enrichment_source,
        CAST(enrichment_date AS TIMESTAMP) as enrichment_date,
        load_date,
        is_manual_load,
        
        -- Extract commonly used fields from flattened DLT columns for CoreSignal data
        CASE 
            WHEN enrichment_source = 'coresignal.com' THEN
                NULLIF(TRIM(enrichment_raw_data__industry), '') 
        END as enriched_industry,
        
        CASE 
            WHEN enrichment_source = 'coresignal.com' THEN
                enrichment_raw_data__employees_count
        END as enriched_employee_count,
        
        CASE 
            WHEN enrichment_source = 'coresignal.com' THEN
                NULLIF(TRIM(enrichment_raw_data__description), '')
        END as enriched_description,
        
        CASE 
            WHEN enrichment_source = 'coresignal.com' THEN
                CASE 
                    WHEN enrichment_raw_data__founded_year ~ '^[0-9]{4}$' 
                    THEN enrichment_raw_data__founded_year::INTEGER
                    ELSE NULL
                END
        END as enriched_founded_year,
        
        -- Extract revenue from flattened columns
        CASE 
            WHEN enrichment_source = 'coresignal.com' THEN
                COALESCE(
                    enrichment_raw_data__revenue_eqg9ranual_revenue__annual_revenue,
                    enrichment_raw_data__revenue_cuiynanual_revenue__annual_revenue
                )
        END as enriched_annual_revenue
        
    FROM source_data
    WHERE company_domain IS NOT NULL 
      AND TRIM(company_domain) != ''
)

SELECT * FROM enrichment_parsed
-- ABOUTME: Staging model for company enrichment data from external APIs
-- ABOUTME: Processes and cleans enrichment data from CoreSignal and other sources

{{ config(materialized = 'view') }}

WITH raw_enrichment AS (
    SELECT * FROM {{ source('raw_data', 'company_enrichment') }}
),

cleaned_enrichment AS (
    SELECT 
        -- Core identification
        company_domain,
        company_name,
        enrichment_source,
        enrichment_date,
        load_date,
        COALESCE(is_seed, FALSE) as is_manual_load,
        
        -- Extract key enrichment fields from raw data
        enrichment_raw_data__industry as enriched_industry,
        enrichment_raw_data__employees_count as enriched_employee_count,
        COALESCE(
            enrichment_raw_data__description_enriched,
            enrichment_raw_data__description
        ) as enriched_description,
        CASE 
            WHEN enrichment_raw_data__founded_year IS NULL OR TRIM(enrichment_raw_data__founded_year) = '' THEN NULL
            ELSE CAST(enrichment_raw_data__founded_year AS INTEGER)
        END as enriched_founded_year,
        enrichment_raw_data__normalized_revenue__range as enriched_annual_revenue,
        
        -- Additional useful fields for frontend
        enrichment_raw_data__hq_city as enriched_hq_city,
        enrichment_raw_data__hq_state as enriched_hq_state,
        enrichment_raw_data__hq_country as enriched_hq_country,
        enrichment_raw_data__linkedin_url as enriched_linkedin_url,
        enrichment_raw_data__website as enriched_website,
        enrichment_raw_data__size_range as enriched_size_range,
        enrichment_raw_data__followers_count_linkedin as enriched_linkedin_followers,
        
        -- Metadata
        enrichment_raw_data__not_found as data_not_found,
        _dlt_load_id,
        _dlt_id
        
    FROM raw_enrichment
    WHERE company_domain IS NOT NULL
      AND company_domain != ''
)

SELECT * FROM cleaned_enrichment
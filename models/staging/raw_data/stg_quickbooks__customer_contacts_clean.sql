/*
ABOUTME: Clean staging model for QuickBooks customer contact data - basic cleaning and standardization only
ABOUTME: Replaces complex business logic version with proper staging layer principles
*/

{{ config(
    materialized = 'view',
    tags = ['customers', 'contacts', 'staging', 'clean']
) }}

WITH raw_customers AS (
    SELECT * FROM {{ source('raw_data', 'xlsx_customer') }}
)

SELECT 
    -- Customer identifiers
    quick_books_internal_id as customer_id,
    NULLIF(TRIM(customer_name), '') as customer_name,
    NULLIF(TRIM(company_name), '') as company_name,
    
    -- Person name fields (basic cleaning only)
    NULLIF(TRIM(first_name), '') as first_name,
    NULLIF(TRIM(last_name), '') as last_name,
    NULLIF(TRIM(title), '') as name_title,
    NULLIF(TRIM(job_title), '') as job_title,
    
    -- Email fields (no splitting - preserve original structure)
    NULLIF(TRIM(main_email), '') as main_email,
    NULLIF(TRIM(cc_email), '') as cc_email,
    
    -- Phone fields (basic cleaning)
    NULLIF(TRIM(main_phone), '') as main_phone,
    NULLIF(TRIM(alt_phone), '') as alt_phone,
    NULLIF(TRIM(work_phone), '') as work_phone,
    NULLIF(TRIM(mobile), '') as mobile_phone,
    NULLIF(TRIM(fax), '') as fax,
    
    -- Address fields (basic cleaning)
    NULLIF(TRIM(billing_address_line_1), '') as billing_address_line_1,
    NULLIF(TRIM(billing_address_city), '') as billing_address_city,
    NULLIF(TRIM(billing_address_state), '') as billing_address_state,
    NULLIF(TRIM(billing_address_postal_code), '') as billing_address_postal_code,
    
    -- Business fields (basic casting)
    customer_type,
    CAST(current_balance AS NUMERIC) as current_balance,
    status as customer_status,
    
    -- Metadata (basic casting)
    CAST(created_date AS DATE) as created_date,
    CAST(modified_date AS DATE) as modified_date,
    is_seed,
    load_date,
    
    CURRENT_TIMESTAMP as processed_at
    
FROM raw_customers
-- Only include customers with meaningful contact data
WHERE quick_books_internal_id IS NOT NULL
  AND (TRIM(COALESCE(customer_name, '')) != '' 
       OR TRIM(COALESCE(main_email, '')) != '' 
       OR TRIM(COALESCE(cc_email, '')) != '')
ORDER BY customer_name
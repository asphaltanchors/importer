/*
ABOUTME: Staging model for trade show leads - basic cleaning and standardization
ABOUTME: Prepares lead data for company matching and attribution analysis
*/

{{ config(
    materialized = 'view',
    tags = ['trade_shows', 'staging']
) }}

WITH raw_leads AS (
    SELECT * FROM {{ source('raw_trade_shows', 'trade_show_leads') }}
),

cleaned_leads AS (
    SELECT
        -- Lead identifiers
        NULLIF(TRIM(lead_id), '') AS lead_id,
        show_name,
        CAST(show_date AS DATE) AS show_date,
        NULLIF(TRIM(show_location), '') AS show_location,
        NULLIF(TRIM(show_rep), '') AS show_rep,

        -- Personal information
        NULLIF(TRIM(first_name), '') AS first_name,
        NULLIF(TRIM(last_name), '') AS last_name,
        CONCAT_WS(' ',
            NULLIF(TRIM(first_name), ''),
            NULLIF(TRIM(last_name), '')
        ) AS full_name,

        -- Contact information
        LOWER(NULLIF(TRIM(email), '')) AS email,
        NULLIF(TRIM(phone), '') AS phone,
        NULLIF(TRIM(company), '') AS company,
        NULL::VARCHAR AS department,  -- Column not materialized by DLT (all NULL values)
        NULLIF(TRIM(job_title), '') AS title,

        -- Address information
        NULLIF(TRIM(address_1), '') AS address_1,
        NULL::VARCHAR AS address_2,  -- Column not materialized by DLT (all NULL values)
        NULLIF(TRIM(city), '') AS city,
        NULLIF(TRIM(state), '') AS state,
        COALESCE(
            NULLIF(TRIM(postal_code__v_text), ''),
            NULLIF(TRIM(CAST(postal_code AS VARCHAR)), '')
        ) AS postal_code,
        NULLIF(TRIM(country), '') AS country,

        -- Metadata
        NULLIF(TRIM(source_id), '') AS source_id,
        NULLIF(TRIM(notes), '') AS notes,
        CAST(created AS TIMESTAMP) AS created_at,
        CAST(updated AS TIMESTAMP) AS updated_at,
        CAST(load_date AS TIMESTAMP) AS load_date,

        -- Extract email domain for company matching
        CASE
            WHEN email IS NOT NULL AND email LIKE '%@%'
            THEN LOWER(TRIM(SPLIT_PART(email, '@', 2)))
            ELSE NULL
        END AS email_domain,

        -- Categorize email type
        CASE
            WHEN email IS NULL THEN 'no_email'
            WHEN LOWER(email) LIKE '%gmail.com%'
                OR LOWER(email) LIKE '%yahoo.com%'
                OR LOWER(email) LIKE '%hotmail.com%'
                OR LOWER(email) LIKE '%outlook.com%'
                OR LOWER(email) LIKE '%aol.com%'
                OR LOWER(email) LIKE '%icloud.com%'
                OR LOWER(email) LIKE '%me.com%'
            THEN 'personal_email'
            ELSE 'business_email'
        END AS email_type,

        -- Data quality flags
        CASE WHEN email IS NOT NULL AND email LIKE '%@%' THEN TRUE ELSE FALSE END AS has_valid_email,
        CASE WHEN company IS NOT NULL AND TRIM(company) != '' THEN TRUE ELSE FALSE END AS has_company_name,
        CASE WHEN phone IS NOT NULL AND TRIM(phone) != '' THEN TRUE ELSE FALSE END AS has_phone

    FROM raw_leads
    WHERE lead_id IS NOT NULL
),

-- Deduplicate by email - keep one lead per email address
-- Prefer leads with original IDs (not generated) and earlier created dates
deduplicated_leads AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY email
            ORDER BY
                CASE WHEN lead_id NOT LIKE 'gen_%' THEN 0 ELSE 1 END,  -- Prefer original IDs
                created_at NULLS LAST,  -- Prefer earlier created dates
                lead_id  -- Tie-breaker
        ) as row_num
    FROM cleaned_leads
    WHERE email IS NOT NULL  -- Only deduplicate valid emails
)

SELECT
    lead_id,
    show_name,
    show_date,
    show_location,
    show_rep,
    first_name,
    last_name,
    full_name,
    email,
    phone,
    company,
    department,
    title,
    address_1,
    address_2,
    city,
    state,
    postal_code,
    country,
    source_id,
    notes,
    created_at,
    updated_at,
    load_date,
    email_domain,
    email_type,
    has_valid_email,
    has_company_name,
    has_phone
FROM deduplicated_leads
WHERE row_num = 1  -- Keep only the first lead per email

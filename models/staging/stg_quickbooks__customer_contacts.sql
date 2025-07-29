/*
ABOUTME: Staging model for extracting person-level contact information from QuickBooks customer data
ABOUTME: Handles name parsing, email extraction, and phone number normalization for individual contacts
*/

{{ config(
    materialized = 'view',
    tags = ['customers', 'contacts', 'staging']
) }}

WITH raw_customers AS (
    SELECT * FROM {{ source('raw_data', 'xlsx_customer') }}
),

-- Extract and clean individual contact information
customer_contacts AS (
    SELECT
        -- Customer identifiers
        quick_books_internal_id as customer_id,
        customer_name,
        company_name,
        
        -- Personal details - clean and normalize names
        NULLIF(TRIM(first_name), '') as first_name,
        NULLIF(TRIM(last_name), '') as last_name,
        NULLIF(TRIM(title), '') as name_title,
        NULLIF(TRIM(job_title), '') as job_title,
        
        -- Construct full name from available parts
        CASE 
            WHEN NULLIF(TRIM(first_name), '') IS NOT NULL AND NULLIF(TRIM(last_name), '') IS NOT NULL
            THEN TRIM(TRIM(first_name) || ' ' || TRIM(last_name))
            WHEN NULLIF(TRIM(first_name), '') IS NOT NULL
            THEN TRIM(first_name)
            WHEN NULLIF(TRIM(last_name), '') IS NOT NULL  
            THEN TRIM(last_name)
            ELSE NULL
        END as full_name,
        
        -- Email addresses - clean and extract domains
        NULLIF(TRIM(main_email), '') as main_email,
        NULLIF(TRIM(cc_email), '') as cc_email,
        
        -- Extract email domains for primary email
        CASE 
            WHEN NULLIF(TRIM(main_email), '') IS NOT NULL THEN
                CASE 
                    WHEN main_email LIKE '%;%' THEN 
                        LOWER(SPLIT_PART(TRIM(SPLIT_PART(main_email, ';', 1)), '@', 2))
                    ELSE 
                        LOWER(SPLIT_PART(TRIM(main_email), '@', 2))
                END
            ELSE NULL
        END as main_email_domain,
        
        -- Extract email domains for cc email
        CASE 
            WHEN NULLIF(TRIM(cc_email), '') IS NOT NULL THEN
                CASE 
                    WHEN cc_email LIKE '%;%' THEN 
                        LOWER(SPLIT_PART(TRIM(SPLIT_PART(cc_email, ';', 1)), '@', 2))
                    ELSE 
                        LOWER(SPLIT_PART(TRIM(cc_email), '@', 2))
                END
            ELSE NULL
        END as cc_email_domain,
        
        -- Phone numbers - clean and normalize
        NULLIF(TRIM(main_phone), '') as main_phone,
        NULLIF(TRIM(alt_phone), '') as alt_phone,
        NULLIF(TRIM(work_phone), '') as work_phone,
        NULLIF(TRIM(mobile), '') as mobile_phone,
        NULLIF(TRIM(fax), '') as fax,
        
        -- Address information (for context)
        billing_address_line_1,
        billing_address_city,
        billing_address_state,
        billing_address_postal_code,
        
        -- Business context
        customer_type,
        current_balance,
        status as customer_status,
        
        -- Metadata
        created_date,
        modified_date,
        is_seed,
        load_date
        
    FROM raw_customers
),

-- Identify records that contain person-level information
contacts_with_person_data AS (
    SELECT
        *,
        -- Determine if this record has meaningful person information
        CASE 
            WHEN full_name IS NOT NULL OR 
                 main_email IS NOT NULL OR 
                 cc_email IS NOT NULL OR
                 main_phone IS NOT NULL OR
                 job_title IS NOT NULL
            THEN TRUE 
            ELSE FALSE 
        END as has_person_data,
        
        -- Determine primary contact method
        CASE 
            WHEN main_email IS NOT NULL THEN 'email'
            WHEN main_phone IS NOT NULL THEN 'phone'
            WHEN cc_email IS NOT NULL THEN 'cc_email'
            ELSE 'none'
        END as primary_contact_method,
        
        -- Flag for data quality
        CASE 
            WHEN full_name IS NOT NULL AND main_email IS NOT NULL THEN 'complete'
            WHEN full_name IS NOT NULL OR main_email IS NOT NULL THEN 'partial'
            ELSE 'minimal'
        END as contact_data_quality
        
    FROM customer_contacts
)

SELECT 
    -- Generate a unique contact identifier
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'main_email', 'full_name']) }} as contact_id,
    
    -- Customer and company identifiers
    customer_id,
    customer_name,
    company_name,
    
    -- Person details
    first_name,
    last_name,
    full_name,
    name_title,
    job_title,
    
    -- Contact information
    main_email,
    cc_email, 
    main_email_domain,
    cc_email_domain,
    main_phone,
    alt_phone,
    work_phone,
    mobile_phone,
    fax,
    primary_contact_method,
    
    -- Address context
    billing_address_line_1,
    billing_address_city,
    billing_address_state,
    billing_address_postal_code,
    
    -- Business context
    customer_type,
    current_balance,
    customer_status,
    
    -- Data quality indicators
    has_person_data,
    contact_data_quality,
    
    -- Metadata
    created_date,
    modified_date,
    is_seed,
    load_date,
    CURRENT_TIMESTAMP as created_at

FROM contacts_with_person_data
-- Only include records with meaningful person information
WHERE has_person_data = TRUE
ORDER BY customer_name, full_name
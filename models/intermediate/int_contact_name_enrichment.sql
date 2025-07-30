/*
ABOUTME: Name enrichment logic for contact records - derives names from original data and email addresses
ABOUTME: Handles missing names by extracting from email prefixes with intelligent fallback logic
*/

{{ config(
    materialized = 'table',
    tags = ['intermediate', 'contacts', 'name_enrichment']
) }}

WITH email_contacts AS (
    SELECT * FROM {{ ref('int_contact_email_parsing') }}
),

-- Enrich names using intelligent derivation logic
names_enriched AS (
    SELECT 
        *,
        
        -- First name logic
        CASE 
            -- Primary contacts get original name if available
            WHEN contact_priority = 'primary' AND original_first_name IS NOT NULL 
            THEN original_first_name
            -- Extract first name from email prefix for additional contacts
            WHEN SPLIT_PART(SPLIT_PART(individual_email, '@', 1), '.', 1) != ''
            THEN INITCAP(SPLIT_PART(SPLIT_PART(individual_email, '@', 1), '.', 1))
            ELSE NULL
        END as derived_first_name,
        
        -- Last name logic  
        CASE 
            -- Primary contacts get original name if available
            WHEN contact_priority = 'primary' AND original_last_name IS NOT NULL 
            THEN original_last_name
            -- Extract last name from email prefix for additional contacts
            WHEN SPLIT_PART(SPLIT_PART(individual_email, '@', 1), '.', 2) != ''
            THEN INITCAP(SPLIT_PART(SPLIT_PART(individual_email, '@', 1), '.', 2))
            ELSE NULL
        END as derived_last_name
        
    FROM email_contacts
),

-- Construct full names and assess name quality
names_finalized AS (
    SELECT 
        *,
        
        -- Construct full name with fallback logic
        CASE 
            WHEN derived_first_name IS NOT NULL AND derived_last_name IS NOT NULL
            THEN TRIM(derived_first_name || ' ' || derived_last_name)
            WHEN derived_first_name IS NOT NULL
            THEN derived_first_name
            WHEN derived_last_name IS NOT NULL  
            THEN derived_last_name
            -- Fallback to email prefix if no name derivation worked
            WHEN SPLIT_PART(individual_email, '@', 1) != ''
            THEN INITCAP(REPLACE(SPLIT_PART(individual_email, '@', 1), '.', ' '))
            ELSE NULL
        END as full_name,
        
        -- Name quality assessment
        CASE 
            WHEN contact_priority = 'primary' AND original_first_name IS NOT NULL AND original_last_name IS NOT NULL
            THEN 'original_complete'
            WHEN contact_priority = 'primary' AND (original_first_name IS NOT NULL OR original_last_name IS NOT NULL)
            THEN 'original_partial'
            WHEN SPLIT_PART(SPLIT_PART(individual_email, '@', 1), '.', 2) != ''
            THEN 'email_derived_complete'
            WHEN SPLIT_PART(SPLIT_PART(individual_email, '@', 1), '.', 1) != ''
            THEN 'email_derived_partial'
            ELSE 'minimal'
        END as name_quality,
        
        -- Name source tracking
        CASE 
            WHEN contact_priority = 'primary' AND (original_first_name IS NOT NULL OR original_last_name IS NOT NULL)
            THEN 'quickbooks_original'
            WHEN SPLIT_PART(SPLIT_PART(individual_email, '@', 1), '.', 1) != ''
            THEN 'email_derived'
            ELSE 'none'
        END as name_source
        
    FROM names_enriched
)

SELECT 
    -- Contact identifiers
    email_contact_id,
    customer_id,
    customer_name,
    company_name,
    
    -- Email context
    individual_email,
    email_domain,
    email_source,
    email_position,
    contact_priority,
    
    -- Enriched name fields
    derived_first_name as first_name,
    derived_last_name as last_name,
    full_name,
    name_title,
    job_title,
    name_quality,
    name_source,
    
    -- Original name fields for reference
    original_first_name,
    original_last_name,
    
    -- Contact methods (only for primary contacts to avoid duplication)
    CASE 
        WHEN contact_priority = 'primary' THEN main_phone 
        ELSE NULL 
    END as main_phone,
    CASE 
        WHEN contact_priority = 'primary' THEN alt_phone 
        ELSE NULL 
    END as alt_phone,
    CASE 
        WHEN contact_priority = 'primary' THEN work_phone 
        ELSE NULL 
    END as work_phone,
    CASE 
        WHEN contact_priority = 'primary' THEN mobile_phone 
        ELSE NULL 
    END as mobile_phone,
    CASE 
        WHEN contact_priority = 'primary' THEN fax 
        ELSE NULL 
    END as fax,
    
    -- Address (only for primary contacts)
    CASE 
        WHEN contact_priority = 'primary' THEN billing_address_line_1 
        ELSE NULL 
    END as billing_address_line_1,
    CASE 
        WHEN contact_priority = 'primary' THEN billing_address_city 
        ELSE NULL 
    END as billing_address_city,
    CASE 
        WHEN contact_priority = 'primary' THEN billing_address_state 
        ELSE NULL 
    END as billing_address_state,
    CASE 
        WHEN contact_priority = 'primary' THEN billing_address_postal_code 
        ELSE NULL 
    END as billing_address_postal_code,
    
    -- Business context
    customer_type,
    current_balance,
    customer_status,
    
    -- Metadata
    created_date,
    modified_date,
    is_seed,
    load_date,
    processed_at,
    email_parsed_at,
    CURRENT_TIMESTAMP as name_enriched_at
    
FROM names_finalized
ORDER BY customer_name, contact_priority, email_position
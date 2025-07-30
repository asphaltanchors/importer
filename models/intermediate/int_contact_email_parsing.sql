/*
ABOUTME: Email parsing and splitting logic for QuickBooks customer contacts
ABOUTME: Handles semicolon-separated emails and creates individual contact records per email
*/

{{ config(
    materialized = 'table',
    tags = ['intermediate', 'contacts', 'email_parsing']
) }}

WITH clean_contacts AS (
    SELECT * FROM {{ ref('stg_quickbooks__customer_contacts_clean') }}
),

-- Split main emails
main_emails_split AS (
    SELECT 
        cc.*,
        TRIM(email_part) as individual_email,
        ROW_NUMBER() OVER (PARTITION BY cc.customer_id ORDER BY ord) as email_position,
        'main' as email_source
    FROM clean_contacts cc
    CROSS JOIN UNNEST(STRING_TO_ARRAY(COALESCE(cc.main_email, ''), ';')) WITH ORDINALITY AS t(email_part, ord)
    WHERE TRIM(email_part) != '' AND email_part IS NOT NULL
),

-- Split CC emails
cc_emails_split AS (
    SELECT 
        cc.*,
        TRIM(email_part) as individual_email,
        ROW_NUMBER() OVER (PARTITION BY cc.customer_id ORDER BY ord) as email_position,
        'cc' as email_source
    FROM clean_contacts cc
    CROSS JOIN UNNEST(STRING_TO_ARRAY(COALESCE(cc.cc_email, ''), ';')) WITH ORDINALITY AS t(email_part, ord)
    WHERE TRIM(email_part) != '' AND email_part IS NOT NULL
),

-- Combine both email sources
all_emails_combined AS (
    SELECT * FROM main_emails_split
    UNION ALL
    SELECT * FROM cc_emails_split
),

-- Validate and clean individual emails
emails_validated AS (
    SELECT 
        *,
        -- Extract email domain
        LOWER(SPLIT_PART(individual_email, '@', 2)) as email_domain,
        
        -- Basic email validation
        CASE 
            WHEN individual_email LIKE '%@%' 
             AND SPLIT_PART(individual_email, '@', 2) != ''
             AND SPLIT_PART(individual_email, '@', 1) != ''
            THEN TRUE 
            ELSE FALSE 
        END as is_valid_email,
        
        -- Business rule filtering
        CASE 
            WHEN LOWER(individual_email) LIKE '%@marketplace.amazon.com' THEN FALSE
            ELSE TRUE
        END as passes_business_rules
        
    FROM all_emails_combined
),

-- Deduplicate emails within each customer
emails_deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, LOWER(individual_email) 
            ORDER BY 
                CASE WHEN email_source = 'main' THEN 1 ELSE 2 END,  -- Prefer main emails
                email_position
        ) as email_rank
    FROM emails_validated
    WHERE is_valid_email = TRUE 
      AND passes_business_rules = TRUE
),

-- Final email contact records
email_contacts AS (
    SELECT 
        -- Generate stable contact identifier (NO email_position to maintain stability)
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'individual_email', 'email_source']) }} as email_contact_id,
        
        -- Customer context
        customer_id,
        customer_name,
        company_name,
        
        -- Email details
        individual_email,
        email_domain,
        email_source,
        email_position,
        
        -- Contact priority based on source and position
        CASE 
            WHEN email_source = 'main' AND email_position = 1 THEN 'primary'
            WHEN email_source = 'main' THEN 'additional_main'
            ELSE 'cc'
        END as contact_priority,
        
        -- Original customer data (preserved for enrichment)
        first_name as original_first_name,
        last_name as original_last_name,
        name_title,
        job_title,
        main_phone,
        alt_phone,
        work_phone,
        mobile_phone,
        fax,
        billing_address_line_1,
        billing_address_city,
        billing_address_state,
        billing_address_postal_code,
        customer_type,
        current_balance,
        customer_status,
        
        -- Metadata
        created_date,
        modified_date,
        is_seed,
        load_date,
        processed_at,
        CURRENT_TIMESTAMP as email_parsed_at
        
    FROM emails_deduplicated
    WHERE email_rank = 1  -- Only keep preferred version of each email
)

SELECT * FROM email_contacts
ORDER BY customer_name, contact_priority, email_position
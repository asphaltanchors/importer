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

-- First, split emails from main_email field
main_emails_split AS (
    SELECT 
        rc.*,
        TRIM(email_part) as individual_email,
        ROW_NUMBER() OVER (PARTITION BY rc.quick_books_internal_id ORDER BY ord) as email_position,
        'main' as email_source
    FROM raw_customers rc
    CROSS JOIN UNNEST(STRING_TO_ARRAY(COALESCE(NULLIF(TRIM(rc.main_email), ''), ''), ';')) WITH ORDINALITY AS t(email_part, ord)
    WHERE TRIM(email_part) != '' AND email_part IS NOT NULL
),

-- Then, split emails from cc_email field
cc_emails_split AS (
    SELECT 
        rc.*,
        TRIM(email_part) as individual_email,
        ROW_NUMBER() OVER (PARTITION BY rc.quick_books_internal_id ORDER BY ord) as email_position,
        'cc' as email_source
    FROM raw_customers rc
    CROSS JOIN UNNEST(STRING_TO_ARRAY(COALESCE(NULLIF(TRIM(rc.cc_email), ''), ''), ';')) WITH ORDINALITY AS t(email_part, ord)
    WHERE TRIM(email_part) != '' AND email_part IS NOT NULL
),

-- Combine both email sources and deduplicate
customer_contacts_split AS (
    SELECT 
        quick_books_internal_id as customer_id,
        customer_name,
        company_name,
        NULLIF(TRIM(first_name), '') as original_first_name,
        NULLIF(TRIM(last_name), '') as original_last_name,
        NULLIF(TRIM(title), '') as name_title,
        NULLIF(TRIM(job_title), '') as job_title,
        individual_email,
        email_position,
        email_source,
        customer_type,
        current_balance,
        status as customer_status,
        main_phone,
        alt_phone, 
        work_phone,
        mobile,
        fax,
        billing_address_line_1,
        billing_address_city,
        billing_address_state,
        billing_address_postal_code,
        created_date,
        modified_date,
        is_seed,
        load_date
    FROM main_emails_split
    
    UNION ALL
    
    SELECT 
        quick_books_internal_id as customer_id,
        customer_name,
        company_name,
        NULLIF(TRIM(first_name), '') as original_first_name,
        NULLIF(TRIM(last_name), '') as original_last_name,
        NULLIF(TRIM(title), '') as name_title,
        NULLIF(TRIM(job_title), '') as job_title,
        individual_email,
        email_position,
        email_source,
        customer_type,
        current_balance,
        status as customer_status,
        main_phone,
        alt_phone, 
        work_phone,
        mobile,
        fax,
        billing_address_line_1,
        billing_address_city,
        billing_address_state,
        billing_address_postal_code,
        created_date,
        modified_date,
        is_seed,
        load_date
    FROM cc_emails_split
),

-- Deduplicate same email addresses within each customer record
customer_contacts_deduped AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, individual_email 
            ORDER BY 
                CASE WHEN email_source = 'main' THEN 1 ELSE 2 END,  -- Prefer main emails
                email_position
        ) as email_rank
    FROM customer_contacts_split
),

-- Assign names and create proper contact records (only keep rank 1 emails)
customer_contacts AS (
    SELECT
        -- Customer identifiers
        customer_id,
        customer_name,
        company_name,
        
        -- Assign names intelligently based on email position and source
        CASE 
            -- First main email gets the original name
            WHEN email_source = 'main' AND email_position = 1 THEN original_first_name
            -- Extract first name from email prefix for additional contacts
            ELSE INITCAP(SPLIT_PART(SPLIT_PART(individual_email, '@', 1), '.', 1))
        END as first_name,
        
        CASE 
            -- First main email gets the original name
            WHEN email_source = 'main' AND email_position = 1 THEN original_last_name
            -- Extract last name from email prefix for additional contacts
            WHEN SPLIT_PART(SPLIT_PART(individual_email, '@', 1), '.', 2) != '' 
            THEN INITCAP(SPLIT_PART(SPLIT_PART(individual_email, '@', 1), '.', 2))
            ELSE NULL
        END as last_name,
        
        name_title,
        job_title,
        
        -- Construct full name
        CASE 
            WHEN email_source = 'main' AND email_position = 1 AND original_first_name IS NOT NULL AND original_last_name IS NOT NULL
            THEN TRIM(original_first_name || ' ' || original_last_name)
            WHEN email_source = 'main' AND email_position = 1 AND original_first_name IS NOT NULL
            THEN original_first_name
            WHEN email_source = 'main' AND email_position = 1 AND original_last_name IS NOT NULL  
            THEN original_last_name
            -- Generate name from email for additional contacts
            WHEN SPLIT_PART(SPLIT_PART(individual_email, '@', 1), '.', 2) != ''
            THEN INITCAP(SPLIT_PART(SPLIT_PART(individual_email, '@', 1), '.', 1)) || ' ' || 
                 INITCAP(SPLIT_PART(SPLIT_PART(individual_email, '@', 1), '.', 2))
            ELSE INITCAP(SPLIT_PART(SPLIT_PART(individual_email, '@', 1), '.', 1))
        END as full_name,
        
        -- Email information
        individual_email as main_email,
        NULL as cc_email,  -- Each split email becomes a main email
        
        -- Extract email domain
        LOWER(SPLIT_PART(individual_email, '@', 2)) as main_email_domain,
        NULL as cc_email_domain,
        
        -- Additional contact context
        email_source,
        email_position,
        
        -- Contact method assignment
        CASE 
            WHEN email_source = 'main' AND email_position = 1 THEN 'primary'
            WHEN email_source = 'main' THEN 'additional_main'
            ELSE 'cc'
        END as contact_priority,
        
        -- Phone numbers - only assign to primary contact to avoid duplication
        CASE 
            WHEN email_source = 'main' AND email_position = 1 
            THEN NULLIF(TRIM(main_phone), '') 
            ELSE NULL 
        END as main_phone,
        CASE 
            WHEN email_source = 'main' AND email_position = 1 
            THEN NULLIF(TRIM(alt_phone), '') 
            ELSE NULL 
        END as alt_phone,
        CASE 
            WHEN email_source = 'main' AND email_position = 1 
            THEN NULLIF(TRIM(work_phone), '') 
            ELSE NULL 
        END as work_phone,
        CASE 
            WHEN email_source = 'main' AND email_position = 1 
            THEN NULLIF(TRIM(mobile), '') 
            ELSE NULL 
        END as mobile_phone,
        CASE 
            WHEN email_source = 'main' AND email_position = 1 
            THEN NULLIF(TRIM(fax), '') 
            ELSE NULL 
        END as fax,
        
        -- Address information (for context) - only for primary contact
        CASE 
            WHEN email_source = 'main' AND email_position = 1 
            THEN billing_address_line_1 
            ELSE NULL 
        END as billing_address_line_1,
        CASE 
            WHEN email_source = 'main' AND email_position = 1 
            THEN billing_address_city 
            ELSE NULL 
        END as billing_address_city,
        CASE 
            WHEN email_source = 'main' AND email_position = 1 
            THEN billing_address_state 
            ELSE NULL 
        END as billing_address_state,
        CASE 
            WHEN email_source = 'main' AND email_position = 1 
            THEN billing_address_postal_code 
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
        load_date
        
    FROM customer_contacts_deduped
    WHERE email_rank = 1  -- Only keep the preferred version of each email
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
    -- Generate a unique contact identifier including email position for splits
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'main_email', 'email_source', 'email_position']) }} as contact_id,
    
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
    
    -- Email splitting context
    email_source,
    email_position,
    contact_priority,
    
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
  -- Exclude Amazon marketplace emails (anonymous, not actionable contacts)
  AND NOT (LOWER(main_email) LIKE '%@marketplace.amazon.com')
ORDER BY customer_name, full_name
-- ABOUTME: Intermediate model mapping customer contacts to consolidated companies via domain logic
-- ABOUTME: Integrates person-level contacts with existing company consolidation and establishes relationship types

{{ config(
    materialized = 'table',
    tags = ['intermediate', 'customers', 'contacts', 'person_mapping']
) }}

-- Get person-level contact data
WITH customer_contacts AS (
    SELECT * FROM {{ ref('stg_quickbooks__customer_contacts') }}
),

-- Use existing customer-company mapping logic for domain consolidation
customer_company_mapping AS (
    SELECT 
        customer_id,
        customer_name,
        company_domain_key,
        domain_type,
        normalized_main_domain,
        normalized_cc_domain,
        is_individual_customer,
        has_valid_domain
    FROM {{ ref('int_quickbooks__customer_company_mapping') }}
),

-- Join contacts with company consolidation
contacts_with_companies AS (
    SELECT 
        -- Contact identifiers
        cc.contact_id,
        cc.customer_id,
        
        -- Person details
        cc.full_name,
        cc.first_name,
        cc.last_name,
        cc.name_title,
        cc.job_title,
        
        -- Contact information
        cc.main_email,
        cc.cc_email,
        cc.main_email_domain,
        cc.cc_email_domain,
        cc.main_phone,
        cc.alt_phone,
        cc.work_phone,
        cc.mobile_phone,
        cc.fax,
        cc.primary_contact_method,
        
        -- Address and business context
        cc.billing_address_line_1,
        cc.billing_address_city,
        cc.billing_address_state,
        cc.billing_address_postal_code,
        cc.customer_type,
        cc.current_balance,
        cc.customer_status,
        
        -- Data quality
        cc.contact_data_quality,
        
        -- Company mapping from existing logic
        ccm.company_domain_key,
        ccm.domain_type,
        ccm.normalized_main_domain,
        ccm.normalized_cc_domain,
        ccm.is_individual_customer,
        ccm.has_valid_domain,
        
        -- Original customer identifiers for reference
        cc.customer_name as source_customer_name,
        cc.company_name as source_company_name,
        
        -- Metadata
        cc.created_date,
        cc.modified_date,
        cc.is_seed,
        cc.load_date
        
    FROM customer_contacts cc
    LEFT JOIN customer_company_mapping ccm ON cc.customer_id = ccm.customer_id
),

-- Deduplicate contacts by email address across customer records (case-insensitive)
-- Keep the best representative record for each unique email
contacts_deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY LOWER(main_email)  -- Case-insensitive email deduplication
            ORDER BY 
                -- Prefer complete contact data
                CASE WHEN contact_data_quality = 'complete' THEN 1
                     WHEN contact_data_quality = 'partial' THEN 2
                     ELSE 3 END,
                -- Prefer records with higher balances (more important customers)
                current_balance DESC NULLS LAST,
                -- Prefer business domains over consumer domains
                CASE WHEN domain_type = 'business' THEN 1
                     WHEN domain_type = 'consumer' THEN 2
                     ELSE 3 END,
                -- Prefer records with more complete contact info
                CASE WHEN full_name IS NOT NULL THEN 1 ELSE 2 END,
                -- Prefer lowercase emails over mixed case for consistency
                CASE WHEN main_email = LOWER(main_email) THEN 1 ELSE 2 END,
                -- Use customer_id as final tiebreaker for consistency
                customer_id
        ) as email_rank
    FROM contacts_with_companies
    WHERE main_email IS NOT NULL  -- Only deduplicate records with emails
),

-- Combine deduplicated email contacts with non-email contacts
contacts_consolidated AS (
    -- Include the best representative for each email
    SELECT * FROM contacts_deduplicated WHERE email_rank = 1
    
    UNION ALL
    
    -- Include contacts without emails (they can't be duplicated by email)
    SELECT 
        *,
        1 as email_rank  -- Add placeholder rank for consistency
    FROM contacts_with_companies 
    WHERE main_email IS NULL
),

-- Determine contact relationship types within each company
contact_relationships AS (
    SELECT 
        *,
        
        -- Determine contact role based on email relationship
        CASE 
            WHEN main_email IS NOT NULL AND cc_email IS NULL THEN 'primary_contact'
            WHEN main_email IS NOT NULL AND cc_email IS NOT NULL THEN 'primary_with_cc'  
            WHEN main_email IS NULL AND cc_email IS NOT NULL THEN 'cc_contact_only'
            WHEN main_email IS NULL AND cc_email IS NULL AND full_name IS NOT NULL THEN 'name_only_contact'
            ELSE 'minimal_contact'
        END as contact_role,
        
        -- Determine if this is likely the main contact for the company
        ROW_NUMBER() OVER (
            PARTITION BY company_domain_key 
            ORDER BY 
                CASE WHEN contact_data_quality = 'complete' THEN 1
                     WHEN contact_data_quality = 'partial' THEN 2
                     ELSE 3 END,
                current_balance DESC NULLS LAST,
                CASE WHEN main_email IS NOT NULL THEN 1 ELSE 2 END,
                customer_id
        ) as company_contact_rank,
        
        -- Count total contacts per company
        COUNT(*) OVER (PARTITION BY company_domain_key) as total_company_contacts,
        
        -- Flags for contact analysis
        CASE WHEN main_email IS NOT NULL THEN TRUE ELSE FALSE END as has_main_email,
        CASE WHEN cc_email IS NOT NULL THEN TRUE ELSE FALSE END as has_cc_email,
        CASE WHEN full_name IS NOT NULL THEN TRUE ELSE FALSE END as has_full_name,
        CASE WHEN job_title IS NOT NULL THEN TRUE ELSE FALSE END as has_job_title,
        CASE WHEN main_phone IS NOT NULL OR alt_phone IS NOT NULL OR 
                  work_phone IS NOT NULL OR mobile_phone IS NOT NULL THEN TRUE ELSE FALSE END as has_phone
        
    FROM contacts_consolidated
),

-- Final person-company relationships
person_company_relationships AS (
    SELECT 
        -- Generate a unique relationship identifier
        {{ dbt_utils.generate_surrogate_key(['contact_id', 'company_domain_key']) }} as person_company_id,
        
        -- Person identifiers
        contact_id as person_id,
        full_name as person_name,
        first_name,
        last_name,
        name_title,
        job_title,
        
        -- Contact details
        main_email,
        cc_email,
        main_phone,
        alt_phone,
        work_phone,
        mobile_phone,
        fax,
        primary_contact_method,
        
        -- Company relationship
        company_domain_key,
        domain_type,
        contact_role,
        company_contact_rank,
        total_company_contacts,
        
        -- Relationship flags
        CASE WHEN company_contact_rank = 1 THEN TRUE ELSE FALSE END as is_primary_company_contact,
        CASE WHEN total_company_contacts > 1 THEN TRUE ELSE FALSE END as company_has_multiple_contacts,
        is_individual_customer,
        has_valid_domain,
        
        -- Contact capabilities
        has_main_email,
        has_cc_email,
        has_full_name,
        has_job_title,
        has_phone,
        
        -- Business context
        customer_type,
        current_balance,
        customer_status,
        contact_data_quality,
        
        -- Source tracking
        customer_id as source_customer_id,
        source_customer_name,
        source_company_name,
        
        -- Geographic context (from customer record)
        billing_address_line_1,
        billing_address_city,
        billing_address_state,
        billing_address_postal_code,
        
        -- Metadata
        created_date,
        modified_date,
        is_seed,
        load_date,
        CURRENT_TIMESTAMP as created_at
        
    FROM contact_relationships
    -- Focus on contacts with valid company mapping
    WHERE company_domain_key IS NOT NULL 
      AND company_domain_key != 'NO_EMAIL_DOMAIN'
)

SELECT * FROM person_company_relationships
ORDER BY company_domain_key, company_contact_rank, person_name
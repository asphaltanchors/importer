/*
ABOUTME: Fixed person-company mapping logic with stable surrogate keys and proper deduplication
ABOUTME: Replaces broken version that included email_position in hash causing key instability
*/

{{ config(
    materialized = 'table',
    tags = ['intermediate', 'customers', 'contacts', 'person_mapping', 'fixed']
) }}

WITH quality_contacts AS (
    SELECT * FROM {{ ref('int_contact_quality_scoring') }}
),

-- Get existing company mapping for domain consolidation
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

-- Join contacts with company consolidation logic
contacts_with_companies AS (
    SELECT 
        qc.*,
        
        -- Company mapping from existing consolidation logic
        ccm.company_domain_key,
        ccm.domain_type,
        ccm.normalized_main_domain,
        ccm.normalized_cc_domain,
        ccm.is_individual_customer,
        ccm.has_valid_domain
        
    FROM quality_contacts qc
    LEFT JOIN customer_company_mapping ccm ON qc.customer_id = ccm.customer_id
),

-- Cross-customer email deduplication (case-insensitive)
-- Keep the best representative record for each unique email
emails_deduplicated AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY LOWER(individual_email)  -- Case-insensitive email deduplication
            ORDER BY 
                -- Prefer higher completeness scores
                completeness_score DESC,
                -- Prefer primary contacts over additional contacts
                CASE WHEN contact_priority = 'primary' THEN 1
                     WHEN contact_priority = 'additional_main' THEN 2
                     ELSE 3 END,
                -- Prefer business domains over consumer domains
                CASE WHEN domain_type = 'business' THEN 1
                     WHEN domain_type = 'consumer' THEN 2
                     ELSE 3 END,
                -- Prefer accounts with higher balances (more important customers)
                current_balance DESC NULLS LAST,
                -- Use customer_id as final tiebreaker for consistency
                customer_id
        ) as email_rank
    FROM contacts_with_companies
    WHERE individual_email IS NOT NULL
      AND company_domain_key IS NOT NULL 
      AND company_domain_key != 'NO_EMAIL_DOMAIN'
),

-- Only keep the best representative for each email address
unique_email_contacts AS (
    SELECT * FROM emails_deduplicated WHERE email_rank = 1
),

-- Determine contact relationships within each company
company_contact_relationships AS (
    SELECT 
        *,
        
        -- Determine contact role based on email and data completeness
        CASE 
            WHEN individual_email IS NOT NULL AND completeness_score >= 75 THEN 'primary_contact'
            WHEN individual_email IS NOT NULL AND completeness_score >= 50 THEN 'secondary_contact'
            WHEN individual_email IS NOT NULL THEN 'email_contact'
            WHEN has_main_phone THEN 'phone_contact'
            WHEN full_name IS NOT NULL THEN 'name_only_contact'
            ELSE 'minimal_contact'
        END as contact_role,
        
        -- Rank contacts within each company
        ROW_NUMBER() OVER (
            PARTITION BY company_domain_key 
            ORDER BY 
                completeness_score DESC,
                CASE WHEN contact_priority = 'primary' THEN 1 ELSE 2 END,
                current_balance DESC NULLS LAST,
                customer_id
        ) as company_contact_rank,
        
        -- Count total contacts per company
        COUNT(*) OVER (PARTITION BY company_domain_key) as total_company_contacts
        
    FROM unique_email_contacts
),

-- Final person-company relationships with stable keys
person_company_relationships AS (
    SELECT 
        -- FIXED: Generate stable relationship identifier (NO email_position)
        {{ dbt_utils.generate_surrogate_key(['email_contact_id', 'company_domain_key']) }} as person_company_id,
        
        -- FIXED: Use stable contact ID as person ID (NO email_position in hash)
        email_contact_id as person_id,
        full_name as person_name,
        first_name,
        last_name,
        name_title,
        job_title,
        
        -- Contact details
        individual_email as main_email,
        NULL as cc_email,  -- Each split email becomes a main email
        email_domain as main_email_domain,
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
        has_email as has_main_email,
        FALSE as has_cc_email,  -- CC emails are split into separate records
        has_full_name,
        has_job_title,
        CASE WHEN has_main_phone OR has_additional_phone THEN TRUE ELSE FALSE END as has_phone,
        
        -- Quality and engagement
        completeness_score,
        contact_data_quality,
        contact_tier,
        engagement_potential,
        email_marketable,
        phone_contactable,
        key_account_contact,
        
        -- Business context
        customer_type,
        current_balance,
        customer_status,
        
        -- Source tracking
        customer_id as source_customer_id,
        customer_name as source_customer_name,
        company_name as source_company_name,
        
        -- Geographic context
        billing_address_line_1,
        billing_address_city,
        billing_address_state,
        billing_address_postal_code,
        
        -- Metadata
        created_date,
        modified_date,
        is_seed,
        load_date,
        processed_at,
        email_parsed_at,
        name_enriched_at,
        quality_scored_at,
        CURRENT_TIMESTAMP as person_mapped_at
        
    FROM company_contact_relationships
)

SELECT * FROM person_company_relationships
ORDER BY company_domain_key, company_contact_rank, person_name
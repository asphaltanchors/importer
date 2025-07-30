/*
ABOUTME: Contact quality assessment and scoring logic for QuickBooks contacts
ABOUTME: Determines data completeness, contact preferences, and marketing/outreach capabilities
*/

{{ config(
    materialized = 'table',
    tags = ['intermediate', 'contacts', 'quality_scoring']
) }}

WITH enriched_contacts AS (
    SELECT * FROM {{ ref('int_contact_name_enrichment') }}
),

-- Assess contact capabilities and data quality
quality_assessed AS (
    SELECT 
        *,
        
        -- Contact capability flags
        CASE WHEN individual_email IS NOT NULL THEN TRUE ELSE FALSE END as has_email,
        CASE WHEN main_phone IS NOT NULL THEN TRUE ELSE FALSE END as has_main_phone,
        CASE WHEN alt_phone IS NOT NULL OR work_phone IS NOT NULL OR mobile_phone IS NOT NULL THEN TRUE ELSE FALSE END as has_additional_phone,
        CASE WHEN full_name IS NOT NULL THEN TRUE ELSE FALSE END as has_full_name,
        CASE WHEN job_title IS NOT NULL THEN TRUE ELSE FALSE END as has_job_title,
        CASE WHEN billing_address_line_1 IS NOT NULL THEN TRUE ELSE FALSE END as has_address,
        
        -- Primary contact method determination
        CASE 
            WHEN individual_email IS NOT NULL THEN 'email'
            WHEN main_phone IS NOT NULL THEN 'phone'
            WHEN alt_phone IS NOT NULL OR work_phone IS NOT NULL OR mobile_phone IS NOT NULL THEN 'phone'
            ELSE 'none'
        END as primary_contact_method,
        
        -- Data completeness scoring (0-100)
        (
            CASE WHEN individual_email IS NOT NULL THEN 25 ELSE 0 END +
            CASE WHEN full_name IS NOT NULL THEN 20 ELSE 0 END +
            CASE WHEN main_phone IS NOT NULL THEN 15 ELSE 0 END +
            CASE WHEN job_title IS NOT NULL THEN 10 ELSE 0 END +
            CASE WHEN billing_address_line_1 IS NOT NULL THEN 10 ELSE 0 END +
            CASE WHEN name_quality IN ('original_complete', 'email_derived_complete') THEN 10 ELSE 5 END +
            CASE WHEN alt_phone IS NOT NULL OR work_phone IS NOT NULL OR mobile_phone IS NOT NULL THEN 5 ELSE 0 END +
            CASE WHEN name_title IS NOT NULL THEN 5 ELSE 0 END
        ) as completeness_score
        
    FROM enriched_contacts
),

-- Classify contacts based on quality and capabilities
contacts_classified AS (
    SELECT 
        *,
        
        -- Overall data quality classification
        CASE 
            WHEN completeness_score >= 75 THEN 'complete'
            WHEN completeness_score >= 50 THEN 'good'
            WHEN completeness_score >= 25 THEN 'partial'
            ELSE 'minimal'
        END as contact_data_quality,
        
        -- Contact tier for business prioritization
        CASE 
            WHEN contact_priority = 'primary' AND completeness_score >= 75 THEN 'high_value_contact'
            WHEN completeness_score >= 75 THEN 'complete_contact'
            WHEN has_email AND completeness_score >= 50 THEN 'email_contact'
            WHEN has_main_phone AND completeness_score >= 40 THEN 'phone_contact'
            ELSE 'basic_contact'
        END as contact_tier,
        
        -- Marketing and outreach capabilities
        CASE 
            WHEN has_email AND customer_status IS NOT NULL THEN TRUE 
            ELSE FALSE 
        END as email_marketable,
        
        CASE 
            WHEN (has_main_phone OR has_additional_phone) AND customer_status IS NOT NULL THEN TRUE 
            ELSE FALSE 
        END as phone_contactable,
        
        CASE 
            WHEN contact_priority = 'primary' AND current_balance > 0 THEN TRUE 
            ELSE FALSE 
        END as key_account_contact,
        
        -- Engagement potential scoring
        CASE 
            WHEN contact_priority = 'primary' AND completeness_score >= 75 AND current_balance > 1000 THEN 'high_engagement'
            WHEN completeness_score >= 60 AND has_email THEN 'medium_engagement'
            WHEN completeness_score >= 40 THEN 'low_engagement'
            ELSE 'minimal_engagement'
        END as engagement_potential
        
    FROM quality_assessed
)

SELECT 
    -- Contact identifiers
    email_contact_id,
    customer_id,
    customer_name,
    company_name,
    
    -- Email and contact context
    individual_email,
    email_domain,
    email_source,
    email_position,
    contact_priority,
    
    -- Name information
    first_name,
    last_name,
    full_name,
    name_title,
    job_title,
    name_quality,
    name_source,
    
    -- Contact methods
    main_phone,
    alt_phone,
    work_phone,
    mobile_phone,
    fax,
    primary_contact_method,
    
    -- Address information
    billing_address_line_1,
    billing_address_city,
    billing_address_state,
    billing_address_postal_code,
    
    -- Business context
    customer_type,
    current_balance,
    customer_status,
    
    -- Quality and capability flags
    has_email,
    has_main_phone,
    has_additional_phone,
    has_full_name,
    has_job_title,
    has_address,
    completeness_score,
    contact_data_quality,
    contact_tier,
    
    -- Marketing and outreach flags
    email_marketable,
    phone_contactable,
    key_account_contact,
    engagement_potential,
    
    -- Metadata
    created_date,
    modified_date,
    is_seed,
    load_date,
    processed_at,
    email_parsed_at,
    name_enriched_at,
    CURRENT_TIMESTAMP as quality_scored_at
    
FROM contacts_classified
ORDER BY completeness_score DESC, customer_name, contact_priority
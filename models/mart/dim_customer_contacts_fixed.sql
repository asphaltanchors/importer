/*
ABOUTME: Fixed customer contacts dimension table with stable surrogate keys and proper architecture
ABOUTME: Replaces the broken version with proper DBT layering and dimensional modeling principles
*/

{{ config(
    materialized = 'table',
    tags = ['mart', 'customers', 'contacts', 'dimension', 'fixed']
) }}

-- Get fixed person-company mapping data
WITH person_mapping AS (
    SELECT * FROM {{ ref('int_customer_person_mapping_fixed') }}
),

-- Get company consolidation data for enrichment
company_data AS (
    SELECT 
        company_domain_key,
        company_name,
        primary_country,
        region,
        total_revenue,
        total_orders,
        business_size_category,
        revenue_category
    FROM {{ ref('int_quickbooks__company_consolidation') }}
),

-- Enrich persons with company context
persons_with_company_context AS (
    SELECT 
        pm.*,
        
        -- Company context
        cd.company_name as consolidated_company_name,
        cd.primary_country as company_country,
        cd.region as company_region,
        cd.total_revenue as company_total_revenue,
        cd.total_orders as company_total_orders,
        cd.business_size_category,
        cd.revenue_category
        
    FROM person_mapping pm
    LEFT JOIN company_data cd ON pm.company_domain_key = cd.company_domain_key
),

-- Create final dimension structure with stable keys
customer_contacts_dimension AS (
    SELECT 
        -- FIXED: Stable dimensional key for fact table joins
        person_id as contact_id,
        
        -- Additional surrogate key for change detection
        {{ dbt_utils.generate_surrogate_key([
            'person_id', 'person_name', 'main_email', 'company_domain_key'
        ]) }} as contact_dim_key,
        
        -- Person identifiers
        person_name as full_name,
        first_name,
        last_name,
        name_title,
        job_title,
        
        -- Contact information structured for easy access
        main_email as primary_email,
        NULL as secondary_email,  -- Split emails create separate records
        main_phone as primary_phone,
        
        -- Additional phone numbers as JSON for flexibility
        JSON_BUILD_OBJECT(
            'alt_phone', alt_phone,
            'work_phone', work_phone, 
            'mobile_phone', mobile_phone,
            'fax', fax
        ) as additional_contact_methods,
        
        primary_contact_method,
        
        -- Company relationship
        company_domain_key,
        COALESCE(consolidated_company_name, source_company_name) as company_name,
        contact_role,
        company_contact_rank,
        is_primary_company_contact,
        company_has_multiple_contacts,
        
        -- Company context for person analytics
        company_country,
        company_region,
        company_total_revenue,
        company_total_orders,
        business_size_category,
        revenue_category,
        
        -- Contact capabilities and flags
        has_main_email,
        has_cc_email,
        has_full_name,
        has_job_title,
        has_phone,
        
        -- Customer classification
        is_individual_customer,
        domain_type,
        customer_type,
        
        -- Address information
        billing_address_line_1 as primary_address,
        billing_address_city as city,
        billing_address_state as state,
        billing_address_postal_code as postal_code,
        
        -- Financial context
        current_balance,
        customer_status,
        
        -- Quality and engagement indicators
        completeness_score,
        contact_data_quality,
        contact_tier,
        engagement_potential,
        
        -- Marketing and outreach flags
        email_marketable,
        phone_contactable,
        key_account_contact,
        
        -- Source tracking
        source_customer_id,
        source_customer_name,
        source_company_name,
        
        -- Metadata
        created_date as customer_created_date,
        modified_date as customer_modified_date,
        is_seed,
        load_date,
        person_mapped_at as contact_processed_at,
        CURRENT_TIMESTAMP as dimension_created_at
        
    FROM persons_with_company_context
)

SELECT * FROM customer_contacts_dimension
ORDER BY 
    company_name, 
    company_contact_rank, 
    full_name
/*
ABOUTME: Customer contacts dimension table providing person-level information for CRM and analytics
ABOUTME: Consolidates person data with company relationships and contact preferences for dashboard consumption
*/

{{ config(
    materialized = 'table',
    tags = ['mart', 'customers', 'contacts', 'dimension']
) }}

-- Get person-company mapping data
WITH person_mapping AS (
    SELECT * FROM {{ ref('int_quickbooks__customer_person_mapping') }}
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

-- Create final dimension structure
customer_contacts_dimension AS (
    SELECT 
        -- Person identifiers
        person_id as contact_id,
        person_name as full_name,
        first_name,
        last_name,
        name_title,
        job_title,
        
        -- Contact information structured for easy access
        main_email as primary_email,
        cc_email as secondary_email,
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
        consolidated_company_name as company_name,
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
        
        -- Data quality indicators
        contact_data_quality,
        CASE 
            WHEN contact_data_quality = 'complete' AND is_primary_company_contact THEN 'high_value_contact'
            WHEN contact_data_quality = 'complete' THEN 'complete_contact'
            WHEN contact_data_quality = 'partial' AND has_main_email THEN 'email_contact'
            WHEN contact_data_quality = 'partial' AND has_phone THEN 'phone_contact'
            ELSE 'basic_contact'
        END as contact_tier,
        
        -- Marketing and outreach flags
        CASE WHEN has_main_email AND COALESCE(customer_status, 'Active') != 'Inactive' THEN TRUE ELSE FALSE END as email_marketable,
        CASE WHEN has_phone AND COALESCE(customer_status, 'Active') != 'Inactive' THEN TRUE ELSE FALSE END as phone_contactable,
        CASE WHEN is_primary_company_contact AND company_total_revenue > 0 THEN TRUE ELSE FALSE END as key_account_contact,
        
        -- Source tracking
        source_customer_id,
        source_customer_name,
        source_company_name,
        
        -- Metadata
        created_date as customer_created_date,
        modified_date as customer_modified_date,
        is_seed,
        load_date,
        created_at as contact_processed_at,
        CURRENT_TIMESTAMP as dimension_created_at
        
    FROM persons_with_company_context
)

SELECT 
    -- Generate a hash for change detection
    {{ dbt_utils.generate_surrogate_key([
        'contact_id', 'full_name', 'primary_email', 'company_domain_key'
    ]) }} as contact_dim_key,
    
    *
    
FROM customer_contacts_dimension
ORDER BY 
    company_name, 
    company_contact_rank, 
    full_name
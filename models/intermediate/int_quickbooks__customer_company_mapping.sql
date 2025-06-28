-- ABOUTME: Intermediate model for customer-company domain mapping and normalization
-- ABOUTME: Handles email domain extraction, domain normalization, and customer name standardization

{{ config(
    materialized = 'table',
    tags = ['intermediate', 'customers', 'domain_mapping']
) }}

-- Extract email domains from customer data
WITH customers_with_domains AS (
    SELECT 
        c.quick_books_internal_id,
        c.customer_name,
        c.company_name,
        c.main_email,
        c.cc_email,
        c.main_phone,
        c.billing_address_line_1,
        c.billing_address_city,
        c.billing_address_state,
        c.billing_address_postal_code,
        c.sales_rep,
        c.terms,
        c.price_level,
        c.current_balance,
        c.status as customer_status,
        CAST(c.created_date AS TIMESTAMP) as customer_created_date,
        CAST(c.modified_date AS TIMESTAMP) as customer_modified_date,
        
        -- Extract primary email domain
        CASE 
            WHEN c.main_email LIKE '%;%' THEN LOWER(SPLIT_PART(TRIM(SPLIT_PART(c.main_email, ';', 1)), '@', 2))
            ELSE LOWER(SPLIT_PART(TRIM(c.main_email), '@', 2))
        END as main_email_domain,
        -- Extract cc email domain  
        CASE 
            WHEN c.cc_email LIKE '%;%' THEN LOWER(SPLIT_PART(TRIM(SPLIT_PART(c.cc_email, ';', 1)), '@', 2))
            ELSE LOWER(SPLIT_PART(TRIM(c.cc_email), '@', 2))
        END as cc_email_domain
    FROM {{ ref('stg_quickbooks__customers') }} c
),

-- Apply domain normalization using Python-generated mapping tables
customers_with_normalized_domains AS (
    SELECT 
        c.*,
        -- Normalize main email domain using mapping table
        COALESCE(
            dm_main.normalized_domain,
            c.main_email_domain
        ) as normalized_main_domain,
        -- Normalize cc email domain using mapping table  
        COALESCE(
            dm_cc.normalized_domain,
            c.cc_email_domain
        ) as normalized_cc_domain,
        -- Select primary domain for company grouping
        COALESCE(
            dm_main.normalized_domain,
            dm_cc.normalized_domain,
            c.main_email_domain,
            c.cc_email_domain,
            'NO_EMAIL_DOMAIN'
        ) as company_domain_key,
        -- Use domain type from mapping table
        COALESCE(
            dm_main.domain_type,
            dm_cc.domain_type,
            CASE 
                WHEN c.main_email_domain IS NOT NULL OR c.cc_email_domain IS NOT NULL THEN 'corporate'
                ELSE 'no_email'
            END
        ) as domain_type
    FROM customers_with_domains c
    LEFT JOIN {{ source('raw_data', 'domain_mapping') }} dm_main 
        ON c.main_email_domain = dm_main.original_domain
    LEFT JOIN {{ source('raw_data', 'domain_mapping') }} dm_cc 
        ON c.cc_email_domain = dm_cc.original_domain
),

-- Apply customer name normalization using Python-generated mapping tables
customers_with_standardized_names AS (
    SELECT 
        c.*,
        -- Use normalized name from mapping table, fallback to original
        COALESCE(
            cnm.normalized_name,
            c.customer_name
        ) as standardized_customer_name,
        -- Track normalization type
        COALESCE(
            cnm.normalization_type,
            'no_change'
        ) as customer_name_normalization_type
    FROM customers_with_normalized_domains c
    LEFT JOIN {{ source('raw_data', 'customer_name_mapping') }} cnm 
        ON c.customer_name = cnm.original_name
)

SELECT 
    -- Customer identifiers
    quick_books_internal_id as customer_id,
    customer_name,
    standardized_customer_name,
    customer_name_normalization_type,
    company_name as customer_company_name,
    
    -- Domain mapping
    company_domain_key,
    domain_type,
    main_email_domain,
    cc_email_domain,
    normalized_main_domain,
    normalized_cc_domain,
    
    -- Customer details
    main_email,
    cc_email,
    main_phone,
    billing_address_line_1,
    billing_address_city,
    billing_address_state,
    billing_address_postal_code,
    sales_rep,
    terms,
    price_level,
    current_balance,
    customer_status,
    customer_created_date,
    customer_modified_date,
    
    -- Classification flags
    CASE WHEN domain_type = 'individual' THEN TRUE ELSE FALSE END as is_individual_customer,
    CASE WHEN company_domain_key = 'NO_EMAIL_DOMAIN' THEN TRUE ELSE FALSE END as is_missing_email,
    CASE WHEN domain_type IN ('corporate', 'individual') THEN TRUE ELSE FALSE END as has_valid_domain,
    
    -- Metadata
    CURRENT_TIMESTAMP as created_at

FROM customers_with_standardized_names
ORDER BY customer_name
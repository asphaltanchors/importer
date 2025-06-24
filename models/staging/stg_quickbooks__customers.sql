/*
ABOUTME: Staging model for QuickBooks customers with normalized country information
ABOUTME: Applies country inference logic based on state/province codes and cleans address data
*/

{{ config(
    materialized = 'view',
    tags = ['customers', 'quickbooks', 'staging']
) }}

WITH raw_customers AS (
    SELECT * FROM {{ source('raw_data', 'customers') }}
),

-- Define state/province mappings for country inference
us_states AS (
    SELECT unnest(ARRAY[
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 
        'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 
        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 
        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 
        'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
        'DC', 'PR', 'VI', 'GU', 'AS', 'MP'  -- territories
    ]) AS state_code
),

canadian_provinces AS (
    SELECT unnest(ARRAY[
        'AB', 'BC', 'MB', 'NB', 'NL', 'NT', 'NS', 'NU', 
        'ON', 'PE', 'QC', 'SK', 'YT'
    ]) AS province_code
),

-- Normalize customer data with country inference
customers_normalized AS (
    SELECT
        -- Core customer information
        customer_name,
        company_name,
        COALESCE(
            NULLIF(TRIM(canonical_name), ''),
            NULLIF(TRIM(company_name), ''),
            NULLIF(TRIM(customer_name), ''),
            'Unknown Customer'
        ) AS normalized_customer_name,
        
        -- Personal details
        title,
        first_name,
        middle_name,
        last_name,
        account_number,
        
        -- Billing address with country normalization
        billing_address_line_1,
        billing_address_line_2,
        billing_address_line_3,
        billing_address_city,
        billing_address_state,
        billing_address_postal_code,
        NULLIF(TRIM(billing_address_country), '') AS billing_address_country_raw,
        
        -- Country inference for billing address
        CASE 
            -- Use explicit country if provided and not empty
            WHEN NULLIF(TRIM(billing_address_country), '') IS NOT NULL 
                THEN CASE
                    WHEN UPPER(TRIM(billing_address_country)) IN ('USA', 'US', 'UNITED STATES') THEN 'United States'
                    WHEN UPPER(TRIM(billing_address_country)) IN ('CANADA', 'CA') THEN 'Canada'
                    WHEN UPPER(TRIM(billing_address_country)) = 'UK' THEN 'United Kingdom'
                    ELSE TRIM(billing_address_country)
                END
            
            -- Infer from state/province if country is empty
            WHEN UPPER(TRIM(billing_address_state)) IN (SELECT state_code FROM us_states) 
                THEN 'United States'
            WHEN UPPER(TRIM(billing_address_state)) IN (SELECT province_code FROM canadian_provinces) 
                THEN 'Canada'
            
            -- Default fallback for empty state and country (assume US for legacy data)
            ELSE 'United States'
        END AS billing_address_country,
        
        -- Shipping address with country normalization
        shipping_address_line_1,
        shipping_address_line_2,
        shipping_address_line_3,
        shipping_address_city,
        shipping_address_state,
        shipping_address_postal_code,
        NULLIF(TRIM(shipping_address_country), '') AS shipping_address_country_raw,
        
        -- Country inference for shipping address
        CASE 
            -- Use explicit country if provided and not empty
            WHEN NULLIF(TRIM(shipping_address_country), '') IS NOT NULL 
                THEN CASE
                    WHEN UPPER(TRIM(shipping_address_country)) IN ('USA', 'US', 'UNITED STATES') THEN 'United States'
                    WHEN UPPER(TRIM(shipping_address_country)) IN ('CANADA', 'CA') THEN 'Canada'
                    WHEN UPPER(TRIM(shipping_address_country)) = 'UK' THEN 'United Kingdom'
                    ELSE TRIM(shipping_address_country)
                END
            
            -- Infer from state/province if country is empty
            WHEN UPPER(TRIM(shipping_address_state)) IN (SELECT state_code FROM us_states) 
                THEN 'United States'
            WHEN UPPER(TRIM(shipping_address_state)) IN (SELECT province_code FROM canadian_provinces) 
                THEN 'Canada'
            
            -- Default fallback for empty state and country (assume US for legacy data)
            ELSE 'United States'
        END AS shipping_address_country,
        
        -- Primary country (billing takes precedence, shipping as fallback)
        COALESCE(
            CASE 
                WHEN NULLIF(TRIM(billing_address_country), '') IS NOT NULL 
                    THEN CASE
                        WHEN UPPER(TRIM(billing_address_country)) IN ('USA', 'US', 'UNITED STATES') THEN 'United States'
                        WHEN UPPER(TRIM(billing_address_country)) IN ('CANADA', 'CA') THEN 'Canada'
                        WHEN UPPER(TRIM(billing_address_country)) = 'UK' THEN 'United Kingdom'
                        ELSE TRIM(billing_address_country)
                    END
                WHEN UPPER(TRIM(billing_address_state)) IN (SELECT state_code FROM us_states) 
                    THEN 'United States'
                WHEN UPPER(TRIM(billing_address_state)) IN (SELECT province_code FROM canadian_provinces) 
                    THEN 'Canada'
                ELSE NULL
            END,
            CASE 
                WHEN NULLIF(TRIM(shipping_address_country), '') IS NOT NULL 
                    THEN CASE
                        WHEN UPPER(TRIM(shipping_address_country)) IN ('USA', 'US', 'UNITED STATES') THEN 'United States'
                        WHEN UPPER(TRIM(shipping_address_country)) IN ('CANADA', 'CA') THEN 'Canada'
                        WHEN UPPER(TRIM(shipping_address_country)) = 'UK' THEN 'United Kingdom'
                        ELSE TRIM(shipping_address_country)
                    END
                WHEN UPPER(TRIM(shipping_address_state)) IN (SELECT state_code FROM us_states) 
                    THEN 'United States'
                WHEN UPPER(TRIM(shipping_address_state)) IN (SELECT province_code FROM canadian_provinces) 
                    THEN 'Canada'
                ELSE 'United States'  -- Final fallback
            END
        ) AS primary_country,
        
        -- Contact information
        preferred_delivery_method,
        preferred_payment_method,
        main_phone,
        alt_phone,
        work_phone,
        main_email,
        cc_email,
        mobile,
        fax,
        
        -- Business information
        customer_type,
        job_title,
        class,
        currency,
        terms,
        credit_limit,
        price_level,
        sales_rep,
        tax_code,
        tax_item,
        resale_no,
        
        -- Job information
        job_description,
        job_type,
        job_status,
        job_start_date,
        job_projected_end_date,
        job_end_date,
        
        -- Financial
        current_balance,
        status,
        
        -- Metadata
        created_date,
        modified_date,
        quick_books_internal_id,
        industry,
        source_channel,
        load_date,
        is_backup,
        
        -- Additional fields
        notes,
        additional_notes,
        other1,
        other2,
        transx
        
    FROM raw_customers
),

-- Add country categorization for reporting
customers_with_categories AS (
    SELECT 
        *,
        
        -- Country category for dashboard filtering
        CASE 
            WHEN primary_country = 'United States' THEN 'United States'
            WHEN primary_country = 'Canada' THEN 'Canada'
            ELSE 'International'
        END AS country_category,
        
        -- Region grouping
        CASE 
            WHEN primary_country IN ('United States', 'Canada') THEN 'North America'
            ELSE 'International'
        END AS region
        
    FROM customers_normalized
)

SELECT * FROM customers_with_categories
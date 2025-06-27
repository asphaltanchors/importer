/*
ABOUTME: Staging model for QuickBooks customers with normalized country information
ABOUTME: Applies country inference logic based on state/province codes and cleans address data
*/

{{ config(
    materialized = 'view',
    tags = ['customers', 'quickbooks', 'staging']
) }}

WITH raw_customers AS (
    SELECT * FROM {{ source('raw_data', 'xlsx_customer') }}
),

-- Normalize customer data with country inference
customers_normalized AS (
    SELECT
        -- Core customer information
        customer_name,
        company_name,
        COALESCE(
            NULLIF(TRIM(company_name), ''),
            NULLIF(TRIM(customer_name), ''),
            'Unknown Customer'
        ) AS normalized_customer_name,
        
        -- Personal details
        first_name,
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
        
        -- Country inference for billing address using macro
        {{ normalize_billing_country('billing_address_country', 'billing_address_state') }} AS billing_address_country,
        
        -- Shipping address with country normalization
        shipping_address_line_1,
        shipping_address_line_2,
        shipping_address_line_3,
        shipping_address_city,
        shipping_address_state,
        shipping_address_postal_code,
        NULLIF(TRIM(shipping_address_country), '') AS shipping_address_country_raw,
        
        -- Country inference for shipping address using macro
        {{ normalize_shipping_country('shipping_address_country', 'shipping_address_state') }} AS shipping_address_country,
        
        -- Primary country using macro
        {{ normalize_country('billing_address_country', 'billing_address_state', 'shipping_address_country', 'shipping_address_state') }} AS primary_country,
        
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
        terms,
        credit_limit,
        price_level,
        sales_rep,
        tax_code,
        tax_item,
        
        -- Financial
        current_balance,
        status,
        
        -- Metadata
        created_date,
        modified_date,
        quick_books_internal_id,
        load_date,
        is_backup,
        
        -- Additional fields
        notes,
        other1
        
    FROM raw_customers
),

-- Add country categorization for reporting
customers_with_categories AS (
    SELECT 
        *,
        
        -- Country category using macro
        {{ country_category('billing_address_country', 'billing_address_state', 'shipping_address_country', 'shipping_address_state') }} AS country_category,
        
        -- Region grouping using macro
        {{ region('billing_address_country', 'billing_address_state', 'shipping_address_country', 'shipping_address_state') }} AS region
        
    FROM customers_normalized
)

SELECT * FROM customers_with_categories
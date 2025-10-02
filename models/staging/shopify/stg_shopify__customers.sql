-- ABOUTME: Staging model for Shopify customers with marketing consent
-- ABOUTME: Cleans and standardizes customer data from raw Shopify API

with source as (
    select * from {{ source('raw_data', 'customers') }}
),

cleaned as (
    select
        -- Primary key
        id as customer_id,

        -- Identity
        email,
        first_name,
        last_name,
        phone,

        -- Marketing consent
        email_marketing_consent__state as email_marketing_status,
        email_marketing_consent__opt_in_level as email_opt_in_level,
        email_marketing_consent__consent_updated_at as email_consent_date,
        sms_marketing_consent__state as sms_marketing_status,

        -- Customer attributes
        verified_email,
        orders_count,
        cast(total_spent as numeric) as total_spent,

        -- Address (default)
        default_address__company as company_name,
        default_address__address1 as address_line1,
        default_address__city as city,
        default_address__province_code as state_code,
        default_address__zip as postal_code,
        default_address__country_code as country_code,

        -- Metadata
        created_at,
        updated_at,
        _dlt_load_id as dlt_load_id

    from source
)

select * from cleaned

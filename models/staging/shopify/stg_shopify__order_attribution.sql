-- ABOUTME: Staging model for Shopify order marketing attribution
-- ABOUTME: Extracts UTM parameters, landing pages, and referral sources

with source as (
    select * from {{ source('raw_data', 'orders') }}
),

attribution as (
    select
        -- Keys
        id as order_id,
        order_number,

        -- Attribution fields
        source_name,
        landing_site,
        referring_site,

        -- Extract UTM parameters from landing_site
        case
            when landing_site like '%utm_source=%' then
                regexp_replace(
                    substring(landing_site from 'utm_source=([^&]*)'),
                    '\\+', ' ', 'g'
                )
        end as utm_source,

        case
            when landing_site like '%utm_medium=%' then
                regexp_replace(
                    substring(landing_site from 'utm_medium=([^&]*)'),
                    '\\+', ' ', 'g'
                )
        end as utm_medium,

        case
            when landing_site like '%utm_campaign=%' then
                regexp_replace(
                    substring(landing_site from 'utm_campaign=([^&]*)'),
                    '\\+', ' ', 'g'
                )
        end as utm_campaign,

        -- Customer marketing acceptance
        buyer_accepts_marketing,
        customer__email_marketing_consent__state as customer_marketing_status,

        -- Session data
        browser_ip,
        client_details__user_agent as user_agent,
        customer_locale,

        -- Metadata
        created_at,
        _dlt_load_id as dlt_load_id

    from source
    where not coalesce(test, false)
)

select * from attribution

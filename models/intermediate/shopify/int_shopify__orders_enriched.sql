-- ABOUTME: Intermediate model enriching Shopify orders with attribution and fulfillment
-- ABOUTME: Single source of truth for enriched Shopify order data

with orders as (
    select * from {{ ref('stg_shopify__orders') }}
),

attribution as (
    select * from {{ ref('stg_shopify__order_attribution') }}
),

fulfillments as (
    select
        order_id,
        max(fulfillment_status) as fulfillment_status,
        max(tracking_number) as tracking_number,
        max(tracking_company) as tracking_company,
        max(fulfilled_at) as fulfilled_at
    from {{ ref('stg_shopify__order_fulfillments') }}
    group by order_id
),

enriched as (
    select
        -- Order core
        o.*,

        -- Attribution
        a.source_name,
        a.landing_site,
        a.referring_site,
        a.utm_source,
        a.utm_medium,
        a.utm_campaign,
        a.buyer_accepts_marketing,
        a.user_agent,

        -- Fulfillment
        f.fulfillment_status as current_fulfillment_status,
        f.tracking_number,
        f.tracking_company,
        f.fulfilled_at,

        -- Derived fields
        case
            when a.utm_source is not null then 'Paid'
            when a.referring_site like '%google%' then 'Organic Search'
            when a.referring_site like '%bing%' then 'Organic Search'
            when a.referring_site like '%duckduckgo%' then 'Organic Search'
            when a.referring_site is not null then 'Referral'
            when a.source_name = 'web' then 'Direct'
            else 'Unknown'
        end as acquisition_channel,

        case
            when o.total_discounts > 0 then true
            else false
        end as has_discount,

        case
            when f.tracking_number is not null then true
            else false
        end as has_tracking

    from orders o
    left join attribution a using (order_id)
    left join fulfillments f using (order_id)
)

select * from enriched

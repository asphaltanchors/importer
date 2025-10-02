-- ABOUTME: Fact table for marketing attribution and channel performance
-- ABOUTME: One row per order with complete attribution chain

{{ config(
    materialized = 'table',
    tags = ['marketing', 'shopify']
) }}

with orders_enriched as (
    select * from {{ ref('int_shopify__orders_enriched') }}
)

select
    -- Keys
    order_id,
    order_number_formatted as order_number,
    customer_id,

    -- Dates
    order_created_at::date as order_date,
    date_trunc('month', order_created_at)::date as order_month,

    -- Attribution
    acquisition_channel,
    coalesce(utm_source, 'organic') as utm_source,
    coalesce(utm_medium, 'none') as utm_medium,
    coalesce(utm_campaign, 'none') as utm_campaign,
    landing_site,
    referring_site,

    -- Metrics
    total_price as revenue,
    total_discounts as discount_amount,
    shipping_amount,
    total_tax as tax_amount,

    -- Flags
    has_discount,
    buyer_accepts_marketing,

    -- Customer behavior
    user_agent

from orders_enriched

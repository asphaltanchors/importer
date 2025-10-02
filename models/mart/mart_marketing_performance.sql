-- ABOUTME: Mart table for marketing channel performance metrics
-- ABOUTME: Aggregated by channel, source, medium, campaign for dashboards

{{ config(
    materialized = 'table',
    tags = ['marketing', 'shopify', 'aggregated']
) }}

with attribution as (
    select * from {{ ref('fct_order_attribution') }}
)

select
    -- Dimensions
    order_month,
    acquisition_channel,
    utm_source,
    utm_medium,
    utm_campaign,

    -- Metrics
    count(distinct order_id) as order_count,
    count(distinct customer_id) as customer_count,
    sum(revenue) as total_revenue,
    sum(discount_amount) as total_discounts,
    avg(revenue) as avg_order_value,

    -- Conversion metrics
    sum(case when has_discount then 1 else 0 end) as discounted_orders,
    round(100.0 * sum(case when has_discount then 1 else 0 end) / count(*), 2) as discount_rate,

    sum(case when buyer_accepts_marketing then 1 else 0 end) as marketing_opt_ins,
    round(100.0 * sum(case when buyer_accepts_marketing then 1 else 0 end) / count(*), 2) as opt_in_rate

from attribution
group by 1, 2, 3, 4, 5

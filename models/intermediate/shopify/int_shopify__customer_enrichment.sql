-- ABOUTME: Intermediate model for Shopify customer marketing enrichment
-- ABOUTME: Aggregates order behavior and marketing consent at customer level

with customers as (
    select * from {{ ref('stg_shopify__customers') }}
),

orders as (
    select * from {{ ref('int_shopify__orders_enriched') }}
),

customer_orders as (
    select
        customer_id,
        count(*) as shopify_order_count,
        sum(total_price) as shopify_lifetime_value,
        sum(case when has_discount then 1 else 0 end) as discounted_order_count,
        avg(total_price) as avg_order_value,
        min(order_created_at) as first_order_date,
        max(order_created_at) as last_order_date,
        count(distinct acquisition_channel) as channel_diversity
    from orders
    group by customer_id
),

enriched as (
    select
        c.*,
        coalesce(co.shopify_order_count, 0) as shopify_order_count,
        coalesce(co.shopify_lifetime_value, 0) as shopify_lifetime_value,
        coalesce(co.discounted_order_count, 0) as discounted_order_count,
        coalesce(co.avg_order_value, 0) as avg_order_value,
        co.first_order_date,
        co.last_order_date,
        coalesce(co.channel_diversity, 0) as channel_diversity,

        -- Marketing flags
        case
            when c.email_marketing_status = 'subscribed' then true
            else false
        end as is_email_subscriber,

        case
            when c.sms_marketing_status = 'subscribed' then true
            else false
        end as is_sms_subscriber

    from customers c
    left join customer_orders co using (customer_id)
)

select * from enriched

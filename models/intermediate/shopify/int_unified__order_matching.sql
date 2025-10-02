-- ABOUTME: Intermediate model matching Shopify orders to QuickBooks invoices
-- ABOUTME: Creates unified order spine for cross-system reconciliation

with shopify_orders as (
    select
        order_id as shopify_order_id,
        order_number_formatted as order_number,
        order_created_at,
        total_price as shopify_total,
        customer_email as shopify_email
    from {{ ref('int_shopify__orders_enriched') }}
),

qb_orders as (
    select
        order_number,
        order_date,
        total_amount as qb_total,
        customer as qb_customer
    from {{ ref('int_quickbooks__orders') }}
    where order_number like 'S-%'
),

matched as (
    select
        coalesce(s.order_number, q.order_number) as order_number,
        s.shopify_order_id,
        q.order_number as qb_order_number,

        -- Dates
        s.order_created_at as shopify_date,
        q.order_date as qb_date,

        -- Amounts
        s.shopify_total,
        q.qb_total,
        abs(coalesce(s.shopify_total, 0) - coalesce(q.qb_total, 0)) as amount_difference,

        -- Customer references
        s.shopify_email,
        q.qb_customer,

        -- Match status
        case
            when s.order_number is not null and q.order_number is not null then 'MATCHED'
            when s.order_number is not null and q.order_number is null then 'SHOPIFY_ONLY'
            when s.order_number is null and q.order_number is not null then 'QB_ONLY'
        end as match_status,

        case
            when abs(coalesce(s.shopify_total, 0) - coalesce(q.qb_total, 0)) < 0.01 then true
            else false
        end as amounts_match

    from shopify_orders s
    full outer join qb_orders q using (order_number)
)

select * from matched

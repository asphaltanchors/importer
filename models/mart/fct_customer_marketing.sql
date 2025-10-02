-- ABOUTME: Fact table for customer marketing status and engagement
-- ABOUTME: One row per customer with aggregated marketing metrics

{{ config(
    materialized = 'table',
    tags = ['marketing', 'shopify']
) }}

with customers as (
    select * from {{ ref('int_shopify__customer_enrichment') }}
)

select
    -- Keys
    customer_id,
    email,

    -- Identity
    first_name || ' ' || last_name as full_name,
    company_name,

    -- Marketing consent
    is_email_subscriber,
    is_sms_subscriber,
    email_consent_date,

    -- Engagement metrics
    shopify_order_count,
    shopify_lifetime_value,
    avg_order_value,
    discounted_order_count,
    round(100.0 * discounted_order_count / nullif(shopify_order_count, 0), 2) as discount_usage_rate,

    -- Recency
    first_order_date,
    last_order_date,
    current_date - last_order_date::date as days_since_last_order,

    -- Segmentation
    case
        when shopify_lifetime_value > 1000 then 'High Value'
        when shopify_lifetime_value > 500 then 'Medium Value'
        else 'Low Value'
    end as customer_segment,

    case
        when current_date - last_order_date::date <= 30 then 'Active'
        when current_date - last_order_date::date <= 90 then 'At Risk'
        else 'Churned'
    end as customer_status

from customers

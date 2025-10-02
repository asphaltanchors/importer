-- ABOUTME: Staging model for Shopify orders with standardized financial data
-- ABOUTME: Converts string amounts to numeric, cleans status values

with source as (
    select * from {{ source('raw_data', 'orders') }}
),

cleaned as (
    select
        -- Primary keys
        id as order_id,
        order_number,
        'S-' || order_number::text as order_number_formatted,

        -- Dates
        created_at as order_created_at,
        updated_at as order_updated_at,
        processed_at as order_processed_at,
        cancelled_at as order_cancelled_at,

        -- Customer reference
        customer__id as customer_id,
        customer__email as customer_email,
        email as order_email,

        -- Financial data (convert to numeric)
        cast(total_price as numeric) as total_price,
        cast(subtotal_price as numeric) as subtotal_price,
        cast(total_tax as numeric) as total_tax,
        cast(total_discounts as numeric) as total_discounts,
        cast(total_shipping_price_set__shop_money__amount as numeric) as shipping_amount,
        cast(total_tip_received as numeric) as tip_amount,
        currency,

        -- Status fields (standardized)
        upper(financial_status) as financial_status,
        upper(coalesce(fulfillment_status, 'UNFULFILLED')) as fulfillment_status,
        cancel_reason,

        -- Addresses
        billing_address__address1 as billing_address1,
        billing_address__city as billing_city,
        billing_address__province_code as billing_state,
        billing_address__zip as billing_zip,
        billing_address__country_code as billing_country,

        shipping_address__address1 as shipping_address1,
        shipping_address__city as shipping_city,
        shipping_address__province_code as shipping_state,
        shipping_address__zip as shipping_zip,
        shipping_address__country_code as shipping_country,

        -- Flags
        test as is_test_order,
        confirmed as is_confirmed,

        -- Metadata
        _dlt_load_id as dlt_load_id

    from source
    where not coalesce(test, false)  -- Exclude test orders
)

select * from cleaned

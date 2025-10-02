-- ABOUTME: Staging model for Shopify order fulfillments
-- ABOUTME: Flattens nested fulfillment records with tracking numbers

with fulfillments as (
    select * from {{ source('raw_data', 'orders__fulfillments') }}
),

orders as (
    select
        id as order_id,
        _dlt_id as order_dlt_id
    from {{ source('raw_data', 'orders') }}
),

joined as (
    select
        -- Keys
        o.order_id,
        f.id as fulfillment_id,

        -- Fulfillment details
        f.status as fulfillment_status,
        f.tracking_company,
        f.tracking_number,
        f.shipment_status,

        -- Dates
        f.created_at as fulfilled_at,
        f.updated_at as fulfillment_updated_at

    from fulfillments f
    inner join orders o on f._dlt_parent_id = o.order_dlt_id
)

select * from joined

-- ABOUTME: Staging model for Shopify products
-- ABOUTME: Cleans product data and prepares for SKU matching with QuickBooks

with products as (
    select * from {{ source('raw_data', 'products') }}
),

variants as (
    select * from {{ source('raw_data', 'products__variants') }}
),

products_clean as (
    select
        p.id as product_id,
        p.title as product_title,
        p.vendor,
        p.product_type,
        p.status as product_status,
        p.tags,
        p.created_at as product_created_at,
        p.updated_at as product_updated_at,
        p._dlt_id as product_dlt_id,
        p._dlt_load_id as dlt_load_id
    from products p
    where p.status = 'active'
),

variants_clean as (
    select
        v.product_id,
        v.id as variant_id,
        v.title as variant_title,
        v.sku,
        cast(v.price as numeric) as price,
        v.inventory_quantity,
        v.created_at as variant_created_at,
        v._dlt_parent_id as parent_dlt_id
    from variants v
    where v.sku is not null and v.sku != ''
)

select
    p.*,
    v.variant_id,
    v.variant_title,
    v.sku,
    v.price as variant_price,
    v.inventory_quantity
from products_clean p
inner join variants_clean v
    on p.product_dlt_id = v.parent_dlt_id

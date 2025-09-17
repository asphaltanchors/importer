/*
  This fact table model represents products for business consumption.
  Uses consolidated intermediate model to avoid rejoining of upstream concepts.
  
  Fact tables contain:
  - Business entities
  - Foreign keys to dimensions
  - Business metrics/measures
*/

{{ config(
    materialized = 'table',
    tags = ['products', 'quickbooks']
) }}

WITH enriched_items AS (
    -- Use consolidated intermediate model to avoid staging rejoining
    SELECT * FROM {{ ref('int_quickbooks__items_enriched') }}
),

-- Get packaging information for unit calculations
product_packaging AS (
    SELECT * FROM {{ ref('int_quickbooks__product_packaging') }}
),

-- Calculate additional business metrics and add packaging information
products_combined AS (
    SELECT
        -- Primary key
        e.quick_books_internal_id,

        -- Product details
        e.item_name,
        e.sales_description,

        -- Derived attributes (from consolidated intermediate model)
        e.product_family,
        e.material_type,
        e.is_kit,

        -- Additional product information
        e.item_type,
        e.item_subtype,
        e.purchase_description,

        -- Pricing
        COALESCE(e.sales_price, 0) as sales_price,
        COALESCE(e.purchase_cost, 0) as purchase_cost,

        -- Calculated margins
        CASE
            WHEN COALESCE(e.sales_price, 0) > 0
            THEN ROUND(CAST(((COALESCE(e.sales_price, 0) - COALESCE(e.purchase_cost, 0)) / e.sales_price) * 100 AS NUMERIC), 2)
            ELSE 0
        END AS margin_percentage,

        CASE
            WHEN COALESCE(e.sales_price, 0) > 0 OR COALESCE(e.purchase_cost, 0) > 0
            THEN COALESCE(e.sales_price, 0) - COALESCE(e.purchase_cost, 0)
            ELSE 0
        END AS margin_amount,

        -- Product identifiers
        e.manufacturer_s_part_number,

        -- Units
        e.unit_of_measure,

        -- Packaging information for unit sales tracking
        p.packaging_type,
        p.units_per_sku,

        -- Dates
        e.load_date,
        e.snapshot_date

    FROM enriched_items e
    LEFT JOIN product_packaging p
        ON e.item_name = p.item_name
)

SELECT * FROM products_combined

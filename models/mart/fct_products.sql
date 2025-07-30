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

-- Calculate additional business metrics
products_combined AS (
    SELECT
        -- Primary key
        quick_books_internal_id,
        
        -- Product details
        item_name,
        sales_description,
        
        -- Derived attributes (from consolidated intermediate model)
        product_family,
        material_type,
        is_kit,
        
        -- Additional product information
        item_type,
        item_subtype,
        purchase_description,
        
        -- Pricing
        COALESCE(sales_price, 0) as sales_price,
        COALESCE(purchase_cost, 0) as purchase_cost,
        
        -- Calculated margins
        CASE 
            WHEN COALESCE(sales_price, 0) > 0 
            THEN ROUND(CAST(((COALESCE(sales_price, 0) - COALESCE(purchase_cost, 0)) / sales_price) * 100 AS NUMERIC), 2)
            ELSE 0
        END AS margin_percentage,
        
        CASE 
            WHEN COALESCE(sales_price, 0) > 0 OR COALESCE(purchase_cost, 0) > 0
            THEN COALESCE(sales_price, 0) - COALESCE(purchase_cost, 0)
            ELSE 0
        END AS margin_amount,
        
        -- Product identifiers
        manufacturer_s_part_number,

        -- Units
        unit_of_measure,
        
        -- Dates
        load_date,
        snapshot_date
        
    FROM enriched_items
)

SELECT * FROM products_combined

/*
  This fact table model represents products for business consumption.
  It combines data from multiple intermediate models to provide a comprehensive view of products.
  
  Fact tables contain:
  - Business entities
  - Foreign keys to dimensions
  - Business metrics/measures
*/

{{ config(
    materialized = 'table',
    tags = ['products', 'quickbooks']
) }}

WITH items AS (
    -- Get distinct items to ensure one row per item_name
    -- Use ROW_NUMBER to pick the most recent record for each item_name
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY item_name 
            ORDER BY load_date DESC, snapshot_date DESC
        ) as row_num
    FROM {{ ref('stg_quickbooks__items') }}
    WHERE item_name IS NOT NULL AND item_name != ''
),

-- Filter to only the most recent record for each item_name
distinct_items AS (
    SELECT * FROM items WHERE row_num = 1
),

product_family AS (
    SELECT * FROM {{ ref('int_quickbooks__product_family') }}
),

material_type AS (
    SELECT * FROM {{ ref('int_quickbooks__material_type') }}
),

item_kits AS (
    SELECT * FROM {{ ref('int_quickbooks__item_kits') }}
),

-- Join all the intermediate models to create the final fact table
products_combined AS (
    SELECT
        -- Primary key
        i.quick_books_internal_id,
        
        -- Product details
        i.item_name,
        i.sales_description,
        
        -- Derived attributes
        pf.product_family,
        mt.material_type,
        ik.is_kit,
        
        -- Additional product information
        i.item_type,
        i.item_subtype,
        i.purchase_description,
        
        -- Pricing
        i.sales_price,
        i.purchase_cost,
        
        -- Product identifiers
        i.manufacturer_s_part_number,

        -- Units
        i.unit_of_measure,
        
        -- Dates
        i.load_date,
        i.snapshot_date,
        
    FROM distinct_items i
    LEFT JOIN product_family pf ON i.item_name = pf.item_name
    LEFT JOIN material_type mt ON i.item_name = mt.item_name
    LEFT JOIN item_kits ik ON i.item_name = ik.item_name
)

SELECT * FROM products_combined

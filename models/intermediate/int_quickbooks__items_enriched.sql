/*
ABOUTME: Comprehensive intermediate model that enriches item data with all derived attributes
ABOUTME: Consolidates product family, material type, and kit classifications in single model to prevent rejoining violations
*/

{{ config(
    materialized = 'table',
    tags = ['intermediate', 'products', 'quickbooks', 'enriched']
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

-- Apply product family logic (from int_quickbooks__product_family)
items_with_product_family AS (
    SELECT
        *,
        CASE
            -- SP10 family
            WHEN item_name LIKE '01-6310%' THEN 'SP10'
            WHEN item_name IN ('01-7010-FBA', '01-7013.FBA', '01-7010', '01-7013') THEN 'SP10'

            -- SP12 family
            WHEN item_name LIKE '01-6315%' THEN 'SP12'

            -- SP18 family
            WHEN item_name LIKE '01-6318%' THEN 'SP18'

            -- SP58 family
            WHEN item_name LIKE '01-6358%' THEN 'SP58'

            -- AM625 family
            WHEN item_name LIKE '01-7625%' THEN 'AM625'
            WHEN item_name IN ('01-7014-FBA', '71-7010.MST', '01-7014') THEN 'AM625'

            -- Kits family (excluding specific items now assigned to other families)
            WHEN item_name LIKE '%AK4%' OR
                 item_name LIKE '%AK-4%' THEN 'Kits'

            -- Adhesives family (includes all EPX products)
            WHEN item_name LIKE '82-5002%' OR
                 item_name LIKE '82-6002%' OR
                 item_name LIKE '82-6005%' OR
                 sales_description LIKE '%EPX2%' OR
                 sales_description LIKE '%EPX3%' OR
                 sales_description LIKE '%EPX5%' THEN 'Adhesives'

            WHEN item_name LIKE '83-10%' OR
                 item_name LIKE '49-800%' THEN 'Accessories'
            WHEN item_name IN ('01-5390', '82-6002.N', '46-3001') THEN 'Accessories'

            -- Default to 'Uncategorized' for products that don't fit into a family
            ELSE 'Uncategorized'
        END as product_family
    FROM distinct_items
),

-- Apply material type logic (from int_quickbooks__material_type)  
items_with_material_type AS (
    SELECT
        *,
        CASE 
            WHEN item_name IN ('01-6318.7SK', '01-6315.3SK', '01-6315.3SK-2', '01-6358.5SK', '01-6358.5SK-2') THEN 'Stainless Steel'
            WHEN item_name LIKE '01-63%' AND item_name NOT LIKE '%-D' THEN 'Zinc Plated'
            WHEN item_name LIKE '%-D' THEN 'Dacromet'
            WHEN item_name IN ('82-5002.K', '82-5002.010', '82-6002') THEN 'Adhesives'
            WHEN item_name IN ('01-7014', '01-7014-FBA', '01-7625.L') THEN 'Plastic'
            WHEN item_name IN ('01-7011.PST', '01-7010-FBA', '01-7010', '01-7013') THEN 'Zinc Plated'
            WHEN item_name LIKE '01-8003%' THEN 'Tools'
            ELSE 'Uncategorized'
        END AS material_type
    FROM items_with_product_family
),

-- Apply kit detection logic (from int_quickbooks__item_kits)
items_with_kit_detection AS (
    SELECT
        *,
        CASE
            WHEN item_name LIKE '%AK4%' OR
                 item_name LIKE '%AK-4%' OR
                 item_name IN ('01-7010-FBA', '01-7013.FBA', '01-7014-FBA', 
                                '71-7010.MST', '01-7010', '01-7013', '01-7014') THEN TRUE
            ELSE FALSE
        END AS is_kit
    FROM items_with_material_type
)

SELECT
    -- Primary identifiers
    quick_books_internal_id,
    item_name,
    
    -- Basic item information
    item_type,
    item_subtype,
    sales_description,
    purchase_description,
    
    -- Enriched attributes (consolidated from separate intermediate models)
    product_family,
    material_type,
    is_kit,
    
    -- Pricing information
    sales_price,
    purchase_cost,
    
    -- Inventory
    quantity_on_hand,
    quantity_on_order,
    quantity_on_sales_order,
    
    -- Additional attributes
    manufacturer_s_part_number,
    upc,
    unit_of_measure,
    unit_weight_kg,
    status,
    
    -- Dates and metadata
    load_date,
    snapshot_date,
    is_seed
    
FROM items_with_kit_detection
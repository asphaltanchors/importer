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
            -- Electronics and Technology
            WHEN LOWER(item_name) LIKE '%computer%' 
                OR LOWER(item_name) LIKE '%laptop%' 
                OR LOWER(sales_description) LIKE '%electronic%'
                OR LOWER(item_name) LIKE '%ipad%'
                OR LOWER(item_name) LIKE '%tablet%'
                OR LOWER(item_name) LIKE '%phone%'
                OR LOWER(item_name) LIKE '%monitor%'
                OR LOWER(item_name) LIKE '%keyboard%'
                OR LOWER(item_name) LIKE '%mouse%'
                OR LOWER(item_name) LIKE '%cable%'
                OR LOWER(item_name) LIKE '%adapter%'
                OR LOWER(item_name) LIKE '%charger%'
                THEN 'Electronics & Technology'
            
            -- Office Supplies
            WHEN LOWER(item_name) LIKE '%pen%' 
                OR LOWER(item_name) LIKE '%paper%' 
                OR LOWER(item_name) LIKE '%folder%'
                OR LOWER(item_name) LIKE '%binder%'
                OR LOWER(item_name) LIKE '%stapler%'
                OR LOWER(item_name) LIKE '%notebook%'
                OR LOWER(item_name) LIKE '%marker%'
                OR LOWER(item_name) LIKE '%tape%'
                OR LOWER(sales_description) LIKE '%office%'
                THEN 'Office Supplies'
            
            -- Furniture
            WHEN LOWER(item_name) LIKE '%desk%'
                OR LOWER(item_name) LIKE '%chair%'
                OR LOWER(item_name) LIKE '%table%'
                OR LOWER(item_name) LIKE '%cabinet%'
                OR LOWER(item_name) LIKE '%shelf%'
                OR LOWER(sales_description) LIKE '%furniture%'
                THEN 'Furniture'
            
            -- Services
            WHEN LOWER(item_type) LIKE '%service%'
                OR LOWER(item_name) LIKE '%service%'
                OR LOWER(item_name) LIKE '%consultation%'
                OR LOWER(item_name) LIKE '%support%'
                OR LOWER(item_name) LIKE '%maintenance%'
                THEN 'Services'
            
            -- Default category for uncategorized items
            ELSE 'General Products'
        END as product_family
    FROM distinct_items
),

-- Apply material type logic (from int_quickbooks__material_type)  
items_with_material_type AS (
    SELECT
        *,
        CASE
            -- Plastic/Synthetic materials
            WHEN LOWER(item_name) LIKE '%plastic%'
                OR LOWER(sales_description) LIKE '%plastic%'
                OR LOWER(item_name) LIKE '%synthetic%'
                OR LOWER(item_name) LIKE '%polymer%'
                THEN 'Plastic'
            
            -- Metal materials
            WHEN LOWER(item_name) LIKE '%metal%'
                OR LOWER(item_name) LIKE '%steel%'
                OR LOWER(item_name) LIKE '%aluminum%'
                OR LOWER(item_name) LIKE '%iron%'
                OR LOWER(sales_description) LIKE '%metal%'
                THEN 'Metal'
            
            -- Wood materials
            WHEN LOWER(item_name) LIKE '%wood%'
                OR LOWER(item_name) LIKE '%timber%'
                OR LOWER(sales_description) LIKE '%wood%'
                OR LOWER(item_name) LIKE '%oak%'
                OR LOWER(item_name) LIKE '%pine%'
                THEN 'Wood'
            
            -- Fabric/Textile
            WHEN LOWER(item_name) LIKE '%fabric%'
                OR LOWER(item_name) LIKE '%cotton%'
                OR LOWER(item_name) LIKE '%textile%'
                OR LOWER(item_name) LIKE '%cloth%'
                THEN 'Fabric'
            
            -- Glass materials
            WHEN LOWER(item_name) LIKE '%glass%'
                OR LOWER(sales_description) LIKE '%glass%'
                THEN 'Glass'
            
            -- Paper materials
            WHEN LOWER(item_name) LIKE '%paper%'
                OR LOWER(sales_description) LIKE '%paper%'
                THEN 'Paper'
            
            -- Digital/Virtual (for services and software)
            WHEN LOWER(item_type) LIKE '%service%'
                OR LOWER(item_name) LIKE '%software%'
                OR LOWER(item_name) LIKE '%digital%'
                OR LOWER(item_name) LIKE '%online%'
                THEN 'Digital'
            
            -- Default for items where material can't be determined
            ELSE 'Mixed/Unknown'
        END as material_type
    FROM items_with_product_family
),

-- Apply kit detection logic (from int_quickbooks__item_kits)
items_with_kit_detection AS (
    SELECT
        *,
        CASE
            WHEN LOWER(item_name) LIKE '%kit%'
                OR LOWER(item_name) LIKE '%bundle%'
                OR LOWER(item_name) LIKE '%package%'
                OR LOWER(item_name) LIKE '%set%'
                OR LOWER(sales_description) LIKE '%kit%'
                OR LOWER(sales_description) LIKE '%bundle%'
                OR LOWER(sales_description) LIKE '%package%'
                OR LOWER(item_subtype) LIKE '%group%'
                OR LOWER(item_subtype) LIKE '%assembly%'
            THEN TRUE
            ELSE FALSE
        END as is_kit
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
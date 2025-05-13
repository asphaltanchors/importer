/*
  This intermediate model derives product family from item attributes.
  It uses pattern matching on item_name and sales_description to assign product families.
*/

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

product_family_derived AS (
    SELECT
        -- Primary identifiers
        item_name,
        sales_description,
        
        -- Derive product family based on item_name and sales_description patterns
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
            
            -- Default to 'Uncategorized' for products that don't fit into a family
            ELSE 'Uncategorized'
        END AS product_family
    FROM distinct_items
)

SELECT * FROM product_family_derived

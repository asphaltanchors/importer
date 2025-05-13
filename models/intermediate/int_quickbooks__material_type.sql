/*
  This intermediate model derives material type from item attributes.
  It uses pattern matching on item_name to assign material types.
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

material_type_derived AS (
    SELECT
        -- Primary identifiers
        item_name,
        sales_description,
        
        -- Derive material type based on item_name patterns
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
    FROM distinct_items
)

SELECT * FROM material_type_derived

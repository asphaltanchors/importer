/*
  This intermediate model identifies which items are kits based on specific item_name patterns.
  Items with specific naming patterns or in a predefined list are flagged as kits.
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

kits_identified AS (
    SELECT
        -- Primary identifiers
        item_name,
        item_type,
        item_subtype,
        
        -- Identify kits based on item_name patterns
        CASE
            WHEN item_name LIKE '%AK4%' OR
                 item_name LIKE '%AK-4%' OR
                 item_name IN ('01-7010-FBA', '01-7013.FBA', '01-7014-FBA', 
                                '71-7010.MST', '01-7010', '01-7013', '01-7014') THEN TRUE
            ELSE FALSE
        END AS is_kit
    FROM distinct_items
)

SELECT * FROM kits_identified

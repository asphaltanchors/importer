{{ config(materialized='view') }}

-- This view provides a user-friendly way to query item history data
-- It joins with the products table to get additional item information

SELECT
    h.item_name,
    p.sales_description,
    h.column_name,
    h.old_value,
    h.new_value,
    h.changed_at,
    -- Calculate the difference for numeric columns
    CASE 
        WHEN h.column_name IN ('purchase_cost', 'sales_price', 'quantity_on_hand') 
             AND h.old_value IS NOT NULL 
             AND h.new_value IS NOT NULL
        THEN CAST(h.new_value AS DECIMAL) - CAST(h.old_value AS DECIMAL)
        ELSE NULL
    END as numeric_change,
    -- Calculate percent change for numeric columns
    CASE 
        WHEN h.column_name IN ('purchase_cost', 'sales_price') 
             AND h.old_value IS NOT NULL 
             AND h.new_value IS NOT NULL
             AND CAST(h.old_value AS DECIMAL) != 0
        THEN ROUND(((CAST(h.new_value AS DECIMAL) - CAST(h.old_value AS DECIMAL)) / CAST(h.old_value AS DECIMAL)) * 100, 2)
        ELSE NULL
    END as percent_change
FROM {{ ref('item_history') }} h
LEFT JOIN {{ ref('products') }} p ON h.item_name = p.item_name
ORDER BY h.changed_at DESC, h.item_name, h.column_name

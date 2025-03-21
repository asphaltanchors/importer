{{ config(
    materialized='incremental',
    unique_key=['item_name', 'column_name', 'changed_at']
) }}

WITH current_items AS (
    SELECT 
        "Item Name" as item_name,
        "Purchase Cost" as purchase_cost,
        "Sales Price" as sales_price,
        "Quantity On Hand" as quantity_on_hand,
        "Status" as status,
        _sdc_extracted_at
    FROM {{ source('raw', 'items') }}
),

{% if is_incremental() %}
previous_items AS (
    SELECT 
        item_name,
        column_name,
        new_value
    FROM {{ this }}
    WHERE changed_at = (
        SELECT MAX(changed_at) 
        FROM {{ this }}
    )
),

previous_values AS (
    SELECT 
        item_name,
        MAX(CASE WHEN column_name = 'purchase_cost' THEN new_value END) as purchase_cost,
        MAX(CASE WHEN column_name = 'sales_price' THEN new_value END) as sales_price,
        MAX(CASE WHEN column_name = 'quantity_on_hand' THEN new_value END) as quantity_on_hand,
        MAX(CASE WHEN column_name = 'status' THEN new_value END) as status
    FROM previous_items
    GROUP BY item_name
),

changes AS (
    SELECT
        c.item_name,
        'purchase_cost' as column_name,
        p.purchase_cost as old_value,
        c.purchase_cost as new_value,
        c._sdc_extracted_at as changed_at
    FROM current_items c
    LEFT JOIN previous_values p ON c.item_name = p.item_name
    WHERE (p.purchase_cost IS NULL AND c.purchase_cost IS NOT NULL)
       OR (p.purchase_cost IS NOT NULL AND c.purchase_cost IS NULL)
       OR (p.purchase_cost != c.purchase_cost)
    
    UNION ALL
    
    SELECT
        c.item_name,
        'sales_price' as column_name,
        p.sales_price as old_value,
        c.sales_price as new_value,
        c._sdc_extracted_at as changed_at
    FROM current_items c
    LEFT JOIN previous_values p ON c.item_name = p.item_name
    WHERE (p.sales_price IS NULL AND c.sales_price IS NOT NULL)
       OR (p.sales_price IS NOT NULL AND c.sales_price IS NULL)
       OR (p.sales_price != c.sales_price)
    
    UNION ALL
    
    SELECT
        c.item_name,
        'quantity_on_hand' as column_name,
        p.quantity_on_hand as old_value,
        c.quantity_on_hand as new_value,
        c._sdc_extracted_at as changed_at
    FROM current_items c
    LEFT JOIN previous_values p ON c.item_name = p.item_name
    WHERE (p.quantity_on_hand IS NULL AND c.quantity_on_hand IS NOT NULL)
       OR (p.quantity_on_hand IS NOT NULL AND c.quantity_on_hand IS NULL)
       OR (p.quantity_on_hand != c.quantity_on_hand)
    
    UNION ALL
    
    SELECT
        c.item_name,
        'status' as column_name,
        p.status as old_value,
        c.status as new_value,
        c._sdc_extracted_at as changed_at
    FROM current_items c
    LEFT JOIN previous_values p ON c.item_name = p.item_name
    WHERE (p.status IS NULL AND c.status IS NOT NULL)
       OR (p.status IS NOT NULL AND c.status IS NULL)
       OR (p.status != c.status)
)

SELECT * FROM changes

{% else %}
-- Initial load: capture current state of all items
initial_load AS (
    SELECT
        item_name,
        'purchase_cost' as column_name,
        NULL as old_value,
        purchase_cost as new_value,
        _sdc_extracted_at as changed_at
    FROM current_items
    WHERE purchase_cost IS NOT NULL
    
    UNION ALL
    
    SELECT
        item_name,
        'sales_price' as column_name,
        NULL as old_value,
        sales_price as new_value,
        _sdc_extracted_at as changed_at
    FROM current_items
    WHERE sales_price IS NOT NULL
    
    UNION ALL
    
    SELECT
        item_name,
        'quantity_on_hand' as column_name,
        NULL as old_value,
        quantity_on_hand as new_value,
        _sdc_extracted_at as changed_at
    FROM current_items
    WHERE quantity_on_hand IS NOT NULL
    
    UNION ALL
    
    SELECT
        item_name,
        'status' as column_name,
        NULL as old_value,
        status as new_value,
        _sdc_extracted_at as changed_at
    FROM current_items
    WHERE status IS NOT NULL
)

SELECT * FROM initial_load

{% endif %}

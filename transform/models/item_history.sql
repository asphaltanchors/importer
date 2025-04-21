{{ config(
    materialized='table',
    unique_key=['item_name', 'column_name', 'changed_at']
) }}

WITH changes AS (
    -- Purchase Cost changes
    SELECT
        item_name,
        'purchase_cost' as column_name,
        LAG(purchase_cost) OVER (PARTITION BY item_name ORDER BY dbt_valid_from) as old_value,
        purchase_cost as new_value,
        '{{ var("file_date", dbt_valid_from) }}'::date as changed_at
    FROM {{ ref('items_snapshot') }}
    
    UNION ALL
    
    -- Sales Price changes
    SELECT
        item_name,
        'sales_price' as column_name,
        LAG(sales_price) OVER (PARTITION BY item_name ORDER BY dbt_valid_from) as old_value,
        sales_price as new_value,
        '{{ var("file_date", dbt_valid_from) }}'::date as changed_at
    FROM {{ ref('items_snapshot') }}
    
    UNION ALL
    
    -- Quantity On Hand changes
    SELECT
        item_name,
        'quantity_on_hand' as column_name,
        LAG(quantity_on_hand) OVER (PARTITION BY item_name ORDER BY dbt_valid_from) as old_value,
        quantity_on_hand as new_value,
        '{{ var("file_date", dbt_valid_from) }}'::date as changed_at
    FROM {{ ref('items_snapshot') }}
    
    UNION ALL
    
    -- Status changes
    SELECT
        item_name,
        'status' as column_name,
        LAG(status) OVER (PARTITION BY item_name ORDER BY dbt_valid_from) as old_value,
        status as new_value,
        '{{ var("file_date", dbt_valid_from) }}'::date as changed_at
    FROM {{ ref('items_snapshot') }}
)

SELECT * FROM changes
WHERE old_value IS NULL OR old_value != new_value

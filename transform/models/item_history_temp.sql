{{ config(
    materialized='view'
) }}

-- This is a temporary placeholder model that doesn't depend on snapshots
-- It will be replaced with the actual implementation after snapshots are created
SELECT 
    'placeholder' as item_name,
    'placeholder' as column_name,
    NULL as old_value,
    NULL as new_value,
    CURRENT_TIMESTAMP as changed_at
WHERE 1=0  -- Empty result set

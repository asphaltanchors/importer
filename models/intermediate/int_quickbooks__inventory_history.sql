/*
  This intermediate model processes inventory history from items snapshots.
  It cleans and standardizes inventory data for each item on each date,
  creating a time-series view of inventory quantities.
*/

{{ config(
    materialized = 'incremental',
    unique_key = ['item_name', 'snapshot_date'],
    on_schema_change = 'fail',
    tags = ['inventory', 'quickbooks']
) }}

WITH source AS (
    SELECT * FROM {{ ref('stg_quickbooks__items') }}
),

inventory_records AS (
    SELECT
        -- Business keys
        item_name,
        snapshot_date,
        
        -- Inventory quantities (cast to numeric for calculations)
        CASE 
            WHEN NULLIF(TRIM(quantity_on_hand), '') IS NULL THEN 0
            ELSE CAST(NULLIF(TRIM(quantity_on_hand), '') AS NUMERIC)
        END AS quantity_on_hand,
        
        CASE 
            WHEN NULLIF(TRIM(quantity_on_order), '') IS NULL THEN 0
            ELSE CAST(NULLIF(TRIM(quantity_on_order), '') AS NUMERIC)
        END AS quantity_on_order,
        
        CASE 
            WHEN NULLIF(TRIM(quantity_on_sales_order), '') IS NULL THEN 0
            ELSE CAST(NULLIF(TRIM(quantity_on_sales_order), '') AS NUMERIC)
        END AS quantity_on_sales_order,
        
        -- Item details for context
        item_type,
        item_subtype,
        sales_price,
        purchase_cost,
        status,
        
        -- Metadata
        inventory_date,
        is_backup,
        load_date
        
    FROM source
    WHERE 
        item_name IS NOT NULL 
        AND item_name != ''
        AND snapshot_date IS NOT NULL
        -- Filter out records with no meaningful inventory data
        AND (
            NULLIF(TRIM(quantity_on_hand), '') IS NOT NULL
            OR NULLIF(TRIM(quantity_on_order), '') IS NOT NULL  
            OR NULLIF(TRIM(quantity_on_sales_order), '') IS NOT NULL
        )
),

-- For incremental runs, only process new snapshot dates
filtered_records AS (
    SELECT * FROM inventory_records
    {% if is_incremental() %}
        WHERE snapshot_date > (SELECT MAX(snapshot_date) FROM {{ this }})
    {% endif %}
)

SELECT * FROM filtered_records
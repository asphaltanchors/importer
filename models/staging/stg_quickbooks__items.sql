/*
  This staging model cleans and standardizes the raw items data from QuickBooks.
  It selects only the necessary columns for downstream models and casts data types appropriately.
*/

WITH source AS (
    SELECT * FROM {{ source('raw_data', 'items') }}
),

cleaned AS (
    SELECT
        -- Primary identifiers
        quick_books_internal_id,
        s_no,
        
        -- Item details
        item_name,
        item_type,
        item_subtype,
        
        -- Descriptions
        sales_description,
        purchase_description,
        
        -- Classification
        class,
        
        -- Pricing (cast to numeric for calculations)
        CASE 
            WHEN NULLIF(TRIM(sales_price), '') IS NULL THEN NULL
            ELSE CAST(NULLIF(TRIM(sales_price), '') AS NUMERIC)
        END AS sales_price,
        CASE 
            WHEN NULLIF(TRIM(purchase_cost), '') IS NULL THEN NULL
            ELSE CAST(NULLIF(TRIM(purchase_cost), '') AS NUMERIC)
        END AS purchase_cost,
        
        -- Inventory
        NULLIF(quantity_on_hand, '') AS quantity_on_hand,
        NULLIF(quantity_on_order, '') AS quantity_on_order,
        NULLIF(quantity_on_sales_order, '') AS quantity_on_sales_order,
        
        -- Product identifiers
        manufacturer_s_part_number,
        bar_code,
        upc,
        
        -- Units
        u_m AS unit_of_measure,
        NULLIF(unit_weight_kg, '') AS unit_weight_kg,
        
        -- Status
        status,
        
        -- Dates
        inventory_date,
        load_date,
        snapshot_date,
        
        -- Metadata
        transx AS transaction_id,
        is_backup
    FROM source
)

SELECT * FROM cleaned

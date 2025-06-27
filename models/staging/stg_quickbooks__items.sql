/*
  This staging model cleans and standardizes the raw items data from QuickBooks.
  It selects only the necessary columns for downstream models and casts data types appropriately.
*/

WITH source AS (
    SELECT * FROM {{ source('raw_data', 'xlsx_item') }}
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
        
        -- Pricing (already NUMERIC in XLSX)
        sales_price,
        purchase_cost,
        
        -- Inventory
        quantity_on_hand,
        quantity_on_order,
        quantity_on_sales_order,
        
        -- Product identifiers
        manufacturer_s_part_number,
        upc,
        
        -- Units
        u_m AS unit_of_measure,
        unit_weight_kg,
        
        -- Status
        status,
        
        -- Dates
        load_date,
        snapshot_date,
        
        -- Metadata
        is_backup
    FROM source
)

SELECT * FROM cleaned
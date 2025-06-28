/*
  This fact table provides a complete history of inventory levels by item and date.
  It's designed for dashboard consumption to show inventory trends over time.
  
  One row per item per date with inventory quantities and derived metrics.
*/

{{ config(
    materialized = 'table',
    tags = ['inventory', 'quickbooks', 'history']
) }}

WITH inventory_history AS (
    SELECT * FROM {{ ref('int_quickbooks__inventory_history') }}
),

-- Get the latest product details for each item
latest_product_info AS (
    SELECT 
        item_name,
        sales_description,
        product_family,
        material_type,
        is_kit,
        item_type,
        item_subtype,
        sales_price,
        purchase_cost,
        unit_of_measure
    FROM {{ ref('fct_products') }}
),

-- Add inventory change calculations
inventory_with_changes AS (
    SELECT
        ih.*,
        
        -- Calculate day-over-day inventory changes
        LAG(quantity_on_hand) OVER (
            PARTITION BY item_name 
            ORDER BY snapshot_date
        ) AS previous_quantity_on_hand,
        
        quantity_on_hand - LAG(quantity_on_hand) OVER (
            PARTITION BY item_name 
            ORDER BY snapshot_date
        ) AS quantity_change,
        
        -- Calculate available inventory (on hand - committed to sales orders)
        quantity_on_hand - quantity_on_sales_order AS available_quantity,
        
        -- Calculate total inventory visibility (on hand + on order)
        quantity_on_hand + quantity_on_order AS total_inventory_visibility
        
    FROM inventory_history ih
),

-- Final fact table with enriched product information
final AS (
    SELECT
        -- Business keys
        iwc.item_name,
        DATE(iwc.snapshot_date) AS inventory_date,
        
        -- Inventory quantities
        iwc.quantity_on_hand,
        iwc.quantity_on_order,
        iwc.quantity_on_sales_order,
        iwc.available_quantity,
        iwc.total_inventory_visibility,
        
        -- Inventory changes
        iwc.previous_quantity_on_hand,
        iwc.quantity_change,
        
        -- Product information from latest product data
        lpi.sales_description AS product_description,
        lpi.product_family,
        lpi.material_type,
        lpi.is_kit,
        lpi.item_type,
        lpi.item_subtype,
        lpi.unit_of_measure,
        
        -- Pricing for inventory valuation
        lpi.sales_price,
        lpi.purchase_cost,
        
        -- Inventory value calculations
        CASE 
            WHEN lpi.purchase_cost IS NOT NULL 
            THEN iwc.quantity_on_hand * lpi.purchase_cost
            ELSE NULL
        END AS inventory_value_at_cost,
        
        CASE 
            WHEN lpi.sales_price IS NOT NULL 
            THEN iwc.quantity_on_hand * lpi.sales_price
            ELSE NULL
        END AS inventory_value_at_sales_price,
        
        -- Status and metadata
        iwc.status AS item_status,
        iwc.is_seed,
        iwc.snapshot_date AS original_snapshot_date
        
    FROM inventory_with_changes iwc
    LEFT JOIN latest_product_info lpi ON iwc.item_name = lpi.item_name
    
    -- Only include records with meaningful inventory data
    WHERE iwc.item_name IS NOT NULL 
    AND iwc.item_name != ''
)

SELECT * FROM final
ORDER BY item_name, inventory_date
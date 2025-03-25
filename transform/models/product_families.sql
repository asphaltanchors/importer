{{ config(materialized='table') }}

WITH product_families AS (
    SELECT
        item_name,
        CASE
            -- SP10 family
            WHEN item_name LIKE '01-6310%' THEN 'SP10'
            
            -- SP12 family
            WHEN item_name LIKE '01-6315%' THEN 'SP12'
            
            -- SP18 family
            WHEN item_name LIKE '01-6318%' THEN 'SP18'
            
            -- SP58 family
            WHEN item_name LIKE '01-6358%' THEN 'SP58'
            
            -- AM625 family
            WHEN item_name LIKE '01-7625%' THEN 'AM625'
            
            -- Kits family
            WHEN item_name LIKE '01-7010%' OR 
                 item_name LIKE '01-7013%' OR 
                 item_name LIKE '01-7014%' OR
                 item_name LIKE '%AK4%' OR
                 item_name LIKE '%AK-4%' THEN 'Kits'
            
            -- Adhesives family (includes all EPX products)
            WHEN item_name LIKE '82-5002%' OR 
                 item_name LIKE '82-6002%' OR 
                 item_name LIKE '82-6005%' OR
                 sales_description LIKE '%EPX2%' OR
                 sales_description LIKE '%EPX3%' OR
                 sales_description LIKE '%EPX5%' THEN 'Adhesives'
            
            -- Default to NULL for products that don't fit into a family
            ELSE NULL
        END AS product_family
    FROM {{ ref('products') }}
)

SELECT
    item_name,
    product_family
FROM product_families
WHERE product_family IS NOT NULL
ORDER BY product_family, item_name

{{ config(materialized='table') }}

SELECT
    item_name,
    sales_description,
    CASE
        -- SP10 family
        WHEN item_name LIKE '01-6310%' THEN 'SP10'
        WHEN item_name IN ('01-7010-FBA', '01-7013.FBA', '01-7010', '01-7013') THEN 'SP10'
        
        -- SP12 family
        WHEN item_name LIKE '01-6315%' THEN 'SP12'
        
        -- SP18 family
        WHEN item_name LIKE '01-6318%' THEN 'SP18'
        
        -- SP58 family
        WHEN item_name LIKE '01-6358%' THEN 'SP58'
        
        -- AM625 family
        WHEN item_name LIKE '01-7625%' THEN 'AM625'
        WHEN item_name IN ('01-7014-FBA', '71-7010.MST', '01-7014') THEN 'AM625'
        
        -- Kits family (excluding specific items now assigned to other families)
        WHEN item_name LIKE '%AK4%' OR
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
    END as product_family,
    CASE 
        WHEN item_name IN ('01-6318.7SK', '01-6315.3SK', '01-6315.3SK-2', '01-6358.5SK', '01-6358.5SK-2') THEN 'Stainless Steel'
        WHEN item_name LIKE '01-63%' AND item_name NOT LIKE '%-D' THEN 'Zinc Plated'
        WHEN item_name LIKE '%-D' THEN 'Dacromet'
        WHEN item_name IN ('82-5002.K', '82-5002.010', '82-6002') THEN 'Adhesives'
        WHEN item_name IN ('01-7014', '01-7014-FBA', '01-7625.L') THEN 'Plastic'
        WHEN item_name IN ('01-7011.PST', '01-7010-FBA', '01-7010', '01-7013') THEN 'Zinc Plated'
        WHEN item_name LIKE '01-8003%' THEN 'Tools'
        ELSE NULL
    END as material_type,
    -- Boolean to indicate if the product is a kit
    CASE
        WHEN item_name LIKE '%AK4%' OR
             item_name LIKE '%AK-4%' OR
             item_name IN ('01-7010-FBA', '01-7013.FBA', '01-7014-FBA', 
                            '71-7010.MST', '01-7010', '01-7013', '01-7014') THEN TRUE
        ELSE FALSE
    END as is_kit,
    
    CASE
        -- Special cases with explicit quantities
        WHEN item_name = '01-7625.L' THEN 216  -- 36 packs × 6 items
        WHEN item_name = '01-6310.72L' THEN 72
        WHEN item_name = '01-7013.FBA' THEN 4
        WHEN item_name = '01-6358.58K' THEN 6
        WHEN item_name = '01-7010-FBA' THEN 4
        WHEN item_name = '01-7014-FBA' THEN 4
        WHEN item_name = '01-7010' THEN 4
        WHEN item_name = '01-7013' THEN 4
        WHEN item_name = '01-7014' THEN 4

        -- Extract quantity from carton patterns (prioritized)
        WHEN sales_description ~ '[0-9]+ .* per carton' THEN 
            (regexp_match(sales_description, '([0-9]+) .* per carton', 'i'))[1]::int
        WHEN sales_description ~ 'carton of [0-9]+' THEN 
            (regexp_match(sales_description, 'carton of ([0-9]+)', 'i'))[1]::int
        WHEN sales_description ~ '[0-9]+ in a carton' THEN 
            (regexp_match(sales_description, '([0-9]+) in a carton', 'i'))[1]::int
            
        -- Extract quantity from pack patterns
        WHEN sales_description ~ '[0-9]+-pack' THEN 
            (regexp_match(sales_description, '([0-9]+)-pack', 'i'))[1]::int
        WHEN sales_description ~ 'pack of [0-9]+' THEN 
            (regexp_match(sales_description, 'pack of ([0-9]+)', 'i'))[1]::int
            
        -- Other patterns
        WHEN sales_description ~ 'set of [0-9]+' THEN 
            (regexp_match(sales_description, 'set of ([0-9]+)', 'i'))[1]::int
        WHEN sales_description ~ '[0-9]+ .* per bag' THEN 
            (regexp_match(sales_description, '([0-9]+) .* per bag', 'i'))[1]::int
        WHEN sales_description ~ '[0-9]+ in a bag' THEN 
            (regexp_match(sales_description, '([0-9]+) in a bag', 'i'))[1]::int
        WHEN sales_description ~ 'six-pack' THEN 6
        WHEN sales_description ~ 'holding [0-9]+' THEN 
            (regexp_match(sales_description, 'holding ([0-9]+)', 'i'))[1]::int
            
        -- Default to 1 if no quantity information is found
        ELSE 1
    END as item_quantity
FROM {{ ref('items_snapshot') }}
WHERE dbt_valid_to IS NULL  -- Only current records

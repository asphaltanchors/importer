{{ config(materialized='table') }}

SELECT
    "Item Name" as item_name,
    "Sales Description" as sales_description,
    CASE
        -- SP10 family
        WHEN "Item Name" LIKE '01-6310%' THEN 'SP10'
        WHEN "Item Name" IN ('01-7010-FBA', '01-7013.FBA', '01-7010', '01-7013') THEN 'SP10'
        
        -- SP12 family
        WHEN "Item Name" LIKE '01-6315%' THEN 'SP12'
        
        -- SP18 family
        WHEN "Item Name" LIKE '01-6318%' THEN 'SP18'
        
        -- SP58 family
        WHEN "Item Name" LIKE '01-6358%' THEN 'SP58'
        
        -- AM625 family
        WHEN "Item Name" LIKE '01-7625%' THEN 'AM625'
        WHEN "Item Name" IN ('01-7014-FBA', '71-7010.MST', '01-7014') THEN 'AM625'
        
        -- Kits family (excluding specific items now assigned to other families)
        WHEN "Item Name" LIKE '%AK4%' OR
             "Item Name" LIKE '%AK-4%' THEN 'Kits'
        
        -- Adhesives family (includes all EPX products)
        WHEN "Item Name" LIKE '82-5002%' OR 
             "Item Name" LIKE '82-6002%' OR 
             "Item Name" LIKE '82-6005%' OR
             "Sales Description" LIKE '%EPX2%' OR
             "Sales Description" LIKE '%EPX3%' OR
             "Sales Description" LIKE '%EPX5%' THEN 'Adhesives'
        
        -- Default to NULL for products that don't fit into a family
        ELSE NULL
    END as product_family,
    CASE 
        WHEN "Item Name" IN ('01-6318.7SK', '01-6315.3SK', '01-6315.3SK-2', '01-6358.5SK', '01-6358.5SK-2') THEN 'Stainless Steel'
        WHEN "Item Name" LIKE '01-63%' AND "Item Name" NOT LIKE '%-D' THEN 'Zinc Plated'
        WHEN "Item Name" LIKE '%-D' THEN 'Dacromet'
        WHEN "Item Name" IN ('82-5002.K', '82-5002.010', '82-6002') THEN 'Adhesives'
        WHEN "Item Name" IN ('01-7014', '01-7014-FBA', '01-7625.L') THEN 'Plastic'
        WHEN "Item Name" IN ('01-7011.PST', '01-7010-FBA', '01-7010', '01-7013') THEN 'Zinc Plated'
        WHEN "Item Name" LIKE '01-8003%' THEN 'Tools'
        ELSE NULL
    END as material_type,
    -- Boolean to indicate if the product is a kit
    CASE
        WHEN "Item Name" LIKE '%AK4%' OR
             "Item Name" LIKE '%AK-4%' OR
             "Item Name" IN ('01-7010-FBA', '01-7013.FBA', '01-7014-FBA', 
                            '71-7010.MST', '01-7010', '01-7013', '01-7014') THEN TRUE
        ELSE FALSE
    END as is_kit,
    
    CASE
        -- Special cases with explicit quantities
        WHEN "Item Name" = '01-7625.L' THEN 216  -- 36 packs × 6 items
        WHEN "Item Name" = '01-6310.72L' THEN 72
        WHEN "Item Name" = '01-7013.FBA' THEN 4
        WHEN "Item Name" = '01-6358.58K' THEN 6
        WHEN "Item Name" = '01-7010-FBA' THEN 4
        WHEN "Item Name" = '01-7014-FBA' THEN 4
        WHEN "Item Name" = '01-7010' THEN 4
        WHEN "Item Name" = '01-7013' THEN 4
        WHEN "Item Name" = '01-7014' THEN 4

        -- Extract quantity from carton patterns (prioritized)
        WHEN "Sales Description" ~ '[0-9]+ .* per carton' THEN 
            (regexp_match("Sales Description", '([0-9]+) .* per carton', 'i'))[1]::int
        WHEN "Sales Description" ~ 'carton of [0-9]+' THEN 
            (regexp_match("Sales Description", 'carton of ([0-9]+)', 'i'))[1]::int
        WHEN "Sales Description" ~ '[0-9]+ in a carton' THEN 
            (regexp_match("Sales Description", '([0-9]+) in a carton', 'i'))[1]::int
            
        -- Extract quantity from pack patterns
        WHEN "Sales Description" ~ '[0-9]+-pack' THEN 
            (regexp_match("Sales Description", '([0-9]+)-pack', 'i'))[1]::int
        WHEN "Sales Description" ~ 'pack of [0-9]+' THEN 
            (regexp_match("Sales Description", 'pack of ([0-9]+)', 'i'))[1]::int
            
        -- Other patterns
        WHEN "Sales Description" ~ 'set of [0-9]+' THEN 
            (regexp_match("Sales Description", 'set of ([0-9]+)', 'i'))[1]::int
        WHEN "Sales Description" ~ '[0-9]+ .* per bag' THEN 
            (regexp_match("Sales Description", '([0-9]+) .* per bag', 'i'))[1]::int
        WHEN "Sales Description" ~ '[0-9]+ in a bag' THEN 
            (regexp_match("Sales Description", '([0-9]+) in a bag', 'i'))[1]::int
        WHEN "Sales Description" ~ 'six-pack' THEN 6
        WHEN "Sales Description" ~ 'holding [0-9]+' THEN 
            (regexp_match("Sales Description", 'holding ([0-9]+)', 'i'))[1]::int
            
        -- Default to 1 if no quantity information is found
        ELSE 1
    END as item_quantity
FROM {{ source('raw', 'items') }}

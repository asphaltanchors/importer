{{ config(materialized='table') }}

SELECT
    "Item Name" as item_name,
    "Sales Description" as sales_description,
    CASE
        -- Special cases with explicit quantities
        WHEN "Item Name" = '01-7625.L' THEN 216  -- 36 packs × 6 items
        WHEN "Item Name" = '01-6310.72L' THEN 72
        WHEN "Item Name" = '01-7013.FBA' THEN 4
        WHEN "Item Name" = '01-6358.58K' THEN 6
        WHEN "Item Name" = '01-7010-FBA' THEN 4
        WHEN "Item Name" = '01-7014-FBA' THEN 4

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

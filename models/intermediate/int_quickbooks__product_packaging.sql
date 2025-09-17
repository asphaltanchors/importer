/*
ABOUTME: This intermediate model determines packaging information and units per SKU for products.
ABOUTME: Identifies individual items, 6-packs, kits (4 units), and master cartons with parsed unit counts.
*/

{{ config(
    materialized = 'table',
    tags = ['products', 'packaging']
) }}

WITH base_products AS (
    SELECT DISTINCT
        item_name,
        sales_description,
        unit_of_measure,
        is_kit
    FROM {{ ref('int_quickbooks__items_enriched') }}
    WHERE item_name IS NOT NULL
    AND TRIM(item_name) != ''
),

-- Parse master carton unit counts from descriptions
master_carton_parsing AS (
    SELECT
        item_name,
        sales_description,
        unit_of_measure,
        is_kit,

        -- Extract numbers from master carton descriptions
        CASE
            -- Look for patterns like "72 anchors per carton", "36 for EPX2", "40 EPX2"
            WHEN sales_description ~* '\b(\d+)\s*(anchors?\s*per\s*carton|for\s*EPX2|EPX2)\b'
            THEN CAST(SUBSTRING(sales_description FROM '\b(\d+)\s*(?:anchors?\s*per\s*carton|for\s*EPX2|EPX2)\b') AS INTEGER)

            -- Look for "Master 6 6-packs" = 6 * 6 = 36 units
            WHEN sales_description ~* 'Master\s+(\d+)\s+6-packs'
            THEN CAST(SUBSTRING(sales_description FROM 'Master\s+(\d+)\s+6-packs') AS INTEGER) * 6

            -- Look for generic "carton of X" or "X per box"
            WHEN sales_description ~* '\b(\d+)\s*per\s*box\b'
            THEN CAST(SUBSTRING(sales_description FROM '\b(\d+)\s*per\s*box\b') AS INTEGER)

            ELSE NULL
        END AS parsed_unit_count

    FROM base_products
),

-- Determine packaging type and units per SKU
packaging_classified AS (
    SELECT
        item_name,
        sales_description,
        unit_of_measure,
        is_kit,
        parsed_unit_count,

        -- Classify packaging type
        CASE
            WHEN is_kit = TRUE THEN 'kit'
            WHEN item_name ~ '\.[0-9]+L$'
                 OR sales_description ILIKE '%master carton%'
                 OR sales_description ILIKE '%master%'
                 OR item_name LIKE '%.MST' THEN 'master_carton'
            WHEN item_name ~ '\.[0-9]+K(\s|$|\()'
                 OR unit_of_measure ILIKE '%6-pack%'
                 OR sales_description ILIKE '%carton of 6%' THEN '6-pack'
            WHEN unit_of_measure ILIKE '%each%' THEN 'individual'
            ELSE 'individual'  -- Default
        END AS packaging_type,

        -- Determine units per SKU
        CASE
            WHEN is_kit = TRUE THEN 4
            WHEN item_name ~ '\.[0-9]+L$'
                 OR sales_description ILIKE '%master carton%'
                 OR sales_description ILIKE '%master%'
                 OR item_name LIKE '%.MST' THEN
                COALESCE(parsed_unit_count,
                    CASE
                        -- Default master carton sizes based on patterns observed
                        WHEN sales_description ILIKE '%AK4%' THEN 24  -- 4 units * 6 packs = 24
                        WHEN sales_description ILIKE '%EPX2%' THEN 36  -- Common EPX2 master carton size
                        ELSE 72  -- Default for anchor master cartons
                    END
                )
            WHEN item_name ~ '\.[0-9]+K(\s|$|\()'
                 OR unit_of_measure ILIKE '%6-pack%'
                 OR sales_description ILIKE '%carton of 6%' THEN 6
            ELSE 1  -- Individual items
        END AS units_per_sku

    FROM master_carton_parsing
)

SELECT
    item_name,
    packaging_type,
    units_per_sku,

    -- Additional context for validation
    sales_description,
    unit_of_measure,
    is_kit,
    parsed_unit_count,

    -- Metadata
    CURRENT_TIMESTAMP AS created_at

FROM packaging_classified
ORDER BY item_name
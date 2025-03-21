{{ config(materialized='table') }}

SELECT
    "Item Name" as item_name,
    "Sales Description" as sales_description
FROM {{ source('raw', 'items') }}

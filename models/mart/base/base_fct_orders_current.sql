/*
ABOUTME: Base view that filters fct_orders to exclude future-dated orders.
ABOUTME: This eliminates need for manual date filtering in all dashboard queries.
*/

{{ config(
    materialized='view',
    tags=['orders', 'base', 'current']
) }}

SELECT *
FROM {{ ref('fct_orders') }}
WHERE order_date <= CURRENT_DATE
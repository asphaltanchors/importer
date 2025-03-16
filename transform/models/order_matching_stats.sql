{{ config(materialized='table') }}

-- Diagnostic table to track order matching statistics
WITH order_stats AS (
  SELECT
    COUNT(*) AS total_orders,
    SUM(CASE WHEN company_id IS NOT NULL THEN 1 ELSE 0 END) AS matched_orders,
    SUM(CASE WHEN company_id IS NULL THEN 1 ELSE 0 END) AS unmatched_orders,
    SUM(total_amount) AS total_amount,
    SUM(CASE WHEN company_id IS NOT NULL THEN total_amount ELSE 0 END) AS matched_amount,
    SUM(CASE WHEN company_id IS NULL THEN total_amount ELSE 0 END) AS unmatched_amount
  FROM {{ ref('orders') }}
  WHERE total_amount IS NOT NULL
)

SELECT
  total_orders,
  matched_orders,
  unmatched_orders,
  ROUND((matched_orders::numeric / NULLIF(total_orders, 0)) * 100, 2) AS percent_orders_matched,
  total_amount,
  matched_amount,
  unmatched_amount,
  ROUND((matched_amount / NULLIF(total_amount, 0)) * 100, 2) AS percent_amount_matched,
  CURRENT_TIMESTAMP AS generated_at
FROM order_stats

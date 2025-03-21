{{ config(materialized='table') }}

-- Diagnostic table for order statistics
-- Note: Company matching has been moved to a separate Python process
-- This table now only tracks basic order statistics
WITH order_stats AS (
  SELECT
    COUNT(*) AS total_orders,
    0 AS matched_orders,  -- Set to 0 as matching is now done in Python
    COUNT(*) AS unmatched_orders,
    SUM(total_amount) AS total_amount,
    0 AS matched_amount,  -- Set to 0 as matching is now done in Python
    SUM(total_amount) AS unmatched_amount
  FROM {{ ref('orders') }}
  WHERE total_amount IS NOT NULL
)

SELECT
  total_orders,
  matched_orders,
  unmatched_orders,
  0 AS percent_orders_matched,  -- Set to 0 as matching is now done in Python
  total_amount,
  matched_amount,
  unmatched_amount,
  0 AS percent_amount_matched,  -- Set to 0 as matching is now done in Python
  CURRENT_TIMESTAMP AS generated_at
FROM order_stats

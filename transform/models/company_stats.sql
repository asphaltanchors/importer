{{ config(materialized='view') }}

SELECT
  c.company_id,
  COUNT(DISTINCT cust.quickbooks_id) AS customer_count,
  COALESCE(SUM(o.total_amount), 0) AS total_orders
FROM {{ ref('companies') }} c
-- Get customer count
LEFT JOIN {{ ref('customers') }} cust ON cust.company_id = c.company_id
-- Get order totals directly using company_id from orders
LEFT JOIN {{ ref('orders') }} o ON o.company_id = c.company_id
GROUP BY
  c.company_id

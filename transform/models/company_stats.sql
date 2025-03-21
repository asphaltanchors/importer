{{ config(materialized='view') }}

-- Note: Order matching has been moved to a separate Python process
-- This view now only shows customer counts, order totals will be added back
-- once the Python-based matching is implemented
SELECT
  c.company_id,
  COUNT(DISTINCT cust.quickbooks_id) AS customer_count,
  0 AS total_orders  -- Set to 0 as order-to-company matching is now done in Python
FROM {{ ref('companies') }} c
-- Get customer count
LEFT JOIN {{ ref('customers') }} cust ON cust.company_id = c.company_id
GROUP BY
  c.company_id

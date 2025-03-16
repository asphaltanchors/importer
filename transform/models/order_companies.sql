{{ config(materialized='table') }}

-- Version 3: Optimized exact match + partial match for better performance
-- Extract company names from invoices
WITH invoice_companies AS (
  SELECT
    "QuickBooks Internal Id" AS order_id,
    TRIM("Customer") AS customer_name
  FROM {{ source('raw', 'invoices') }}
  WHERE "QuickBooks Internal Id" != '' AND TRIM("Customer") != ''
),

-- Extract company names from sales receipts
sales_receipt_companies AS (
  SELECT
    "QuickBooks Internal Id" AS order_id,
    TRIM("Customer") AS customer_name
  FROM {{ source('raw', 'sales_receipts') }}
  WHERE "QuickBooks Internal Id" != '' AND TRIM("Customer") != ''
),

-- Combine both sources
order_companies AS (
  SELECT * FROM invoice_companies
  UNION ALL
  SELECT * FROM sales_receipt_companies
),

-- Pre-filter valid companies (optimization)
valid_companies AS (
  SELECT company_id, company_name
  FROM {{ ref('companies') }}
  WHERE company_name != ''
),

-- Exact matches
exact_matches AS (
  SELECT
    oc.order_id,
    c.company_id
  FROM order_companies oc
  JOIN valid_companies c ON oc.customer_name = c.company_name
),

-- Partial matches with ranking
ranked_partial_matches AS (
  SELECT
    oc.order_id,
    c.company_id,
    ROW_NUMBER() OVER (PARTITION BY oc.order_id ORDER BY LENGTH(c.company_name) DESC) as rank
  FROM order_companies oc
  LEFT JOIN exact_matches em ON oc.order_id = em.order_id
  JOIN valid_companies c ON POSITION(c.company_name IN oc.customer_name) > 0
  WHERE em.order_id IS NULL  -- Only for orders without exact matches
    AND LENGTH(c.company_name) > 5  -- Avoid matching very short company names
),

-- Get only the best partial match for each order
best_partial_matches AS (
  SELECT order_id, company_id
  FROM ranked_partial_matches
  WHERE rank = 1
)

-- Combine both match types
SELECT * FROM exact_matches
UNION ALL
SELECT * FROM best_partial_matches

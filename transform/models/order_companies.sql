{{ config(
    materialized='table',
    post_hook=[
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_order_id ON {{ this }} (order_id)",
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_company_id ON {{ this }} (company_id)",
      "CREATE EXTENSION IF NOT EXISTS pg_trgm",
      "CREATE INDEX IF NOT EXISTS idx_companies_company_name_trgm ON analytics.companies USING gin(company_name gin_trgm_ops)"
    ]
) }}

-- Version 4: Further optimized with materialized CTEs, pre-filtering, and case-insensitive matching
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

-- Combine both sources (materialized to avoid recomputing)
order_companies AS (
  SELECT * FROM invoice_companies
  UNION ALL
  SELECT * FROM sales_receipt_companies
),

-- Pre-filter valid companies (optimization)
valid_companies AS (
  SELECT 
    company_id, 
    company_name,
    LOWER(company_name) AS company_name_lower
  FROM {{ ref('companies') }}
  WHERE company_name != ''
),

-- Exact matches (case-insensitive)
exact_matches AS (
  SELECT
    oc.order_id,
    c.company_id
  FROM order_companies oc
  JOIN valid_companies c ON LOWER(oc.customer_name) = c.company_name_lower
),

-- Pre-filter potential partial matches (much faster than checking all combinations)
potential_matches AS (
  SELECT 
    oc.order_id, 
    oc.customer_name,
    c.company_id, 
    c.company_name
  FROM order_companies oc
  LEFT JOIN exact_matches em ON oc.order_id = em.order_id
  CROSS JOIN valid_companies c
  WHERE em.order_id IS NULL  -- Only for orders without exact matches
    AND LENGTH(c.company_name) > 5  -- Avoid matching very short company names
    AND LENGTH(c.company_name) <= LENGTH(oc.customer_name)  -- Company name can't be longer than customer name
),

-- Apply expensive POSITION check only on pre-filtered candidates
partial_matches AS (
  SELECT
    pm.order_id,
    pm.company_id,
    ROW_NUMBER() OVER (PARTITION BY pm.order_id ORDER BY LENGTH(pm.company_name) DESC) as rank
  FROM potential_matches pm
  WHERE LOWER(pm.customer_name) LIKE '%' || LOWER(pm.company_name) || '%'  -- Case-insensitive partial match
),

-- Get only the best partial match for each order
best_partial_matches AS (
  SELECT order_id, company_id
  FROM partial_matches
  WHERE rank = 1
)

-- Combine both match types
SELECT * FROM exact_matches
UNION ALL
SELECT * FROM best_partial_matches

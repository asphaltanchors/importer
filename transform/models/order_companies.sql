{{ config(
    materialized='table',
    post_hook=[
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_order_id ON {{ this }} (order_id)",
      "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_company_id ON {{ this }} (company_id)"
    ]
) }}

-- Version 7: Improved matching with name normalization
WITH 
-- Extract customer names from invoices
invoice_companies AS (
  SELECT
    "QuickBooks Internal Id" AS order_id,
    UPPER(TRIM("Customer")) AS customer_name_upper,
    -- Normalize customer name using the Python-inspired logic
    CASE WHEN 
      POSITION('%' IN "Customer") > 0 OR
      POSITION(':' IN "Customer") > 0 OR
      POSITION('(' IN "Customer") > 0
    THEN
      -- Just normalize case and whitespace for special names
      UPPER(REGEXP_REPLACE(TRIM("Customer"), '\\s+', ' '))
    ELSE
      -- For simple names, handle comma-based names
      CASE WHEN POSITION(',' IN "Customer") > 0 THEN
        -- "Last, First" becomes "FIRST LAST"
        UPPER(
          TRIM(SPLIT_PART("Customer", ',', 2)) || ' ' || 
          TRIM(SPLIT_PART("Customer", ',', 1))
        )
      ELSE
        -- Just normalize case and whitespace
        UPPER(REGEXP_REPLACE(TRIM("Customer"), '\\s+', ' '))
      END
    END AS normalized_name
  FROM {{ source('raw', 'invoices') }}
  WHERE "QuickBooks Internal Id" != '' AND TRIM("Customer") != ''
),

-- Similar extraction for sales receipts
sales_receipt_companies AS (
  SELECT
    "QuickBooks Internal Id" AS order_id,
    UPPER(TRIM("Customer")) AS customer_name_upper,
    -- Normalize customer name using the Python-inspired logic
    CASE WHEN 
      POSITION('%' IN "Customer") > 0 OR
      POSITION(':' IN "Customer") > 0 OR
      POSITION('(' IN "Customer") > 0
    THEN
      -- Just normalize case and whitespace for special names
      UPPER(REGEXP_REPLACE(TRIM("Customer"), '\\s+', ' '))
    ELSE
      -- For simple names, handle comma-based names
      CASE WHEN POSITION(',' IN "Customer") > 0 THEN
        -- "Last, First" becomes "FIRST LAST"
        UPPER(
          TRIM(SPLIT_PART("Customer", ',', 2)) || ' ' || 
          TRIM(SPLIT_PART("Customer", ',', 1))
        )
      ELSE
        -- Just normalize case and whitespace
        UPPER(REGEXP_REPLACE(TRIM("Customer"), '\\s+', ' '))
      END
    END AS normalized_name
  FROM {{ source('raw', 'sales_receipts') }}
  WHERE "QuickBooks Internal Id" != '' AND TRIM("Customer") != ''
),

-- Combine both sources
order_companies AS (
  SELECT * FROM invoice_companies
  UNION ALL
  SELECT * FROM sales_receipt_companies
),

-- 1. First try original exact match (case-insensitive)
exact_matches AS (
  SELECT DISTINCT
    oc.order_id,
    c.company_id
  FROM order_companies oc
  JOIN {{ ref('companies') }} c ON 
    oc.customer_name_upper = c.company_name_upper
),

-- 2. Then try normalized matches
normalized_matches AS (
  SELECT DISTINCT
    oc.order_id,
    c.company_id
  FROM order_companies oc
  JOIN {{ ref('companies') }} c ON 
    oc.normalized_name = c.normalized_name
  WHERE
    oc.order_id NOT IN (SELECT order_id FROM exact_matches)
)

-- Combine both match types
SELECT * FROM exact_matches
UNION ALL
SELECT * FROM normalized_matches

{{ config(materialized='table') }}

SELECT
  "QuickBooks Internal Id" as quickbooks_id,
  "Invoice No" as order_number,
  "Class" as class,
  "Terms" as terms,
  "Status" as status,
  "PO Number" as po_number,
  MAX(CAST(NULLIF("Total Amount", '') AS NUMERIC)) as total_amount  -- Handle empty strings and cast to NUMERIC
FROM {{ source('raw', 'invoices') }}
GROUP BY
  "QuickBooks Internal Id",
  "Invoice No",
  "Class",
  "Terms",
  "Status",
  "PO Number"

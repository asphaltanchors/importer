{{ config(materialized='table') }}

WITH distinct_invoices AS (
  SELECT DISTINCT
    "QuickBooks Internal Id",
    "Invoice No",
    "Class",
    "Terms",
    "Status", 
    "PO Number"
  FROM {{ source('raw', 'invoices') }}
)

SELECT
  "QuickBooks Internal Id" as quickbooks_id,
  "Invoice No" as order_number,
  "Class" as class,
  "Terms" as terms,
  "Status" as status,
  "PO Number" as po_number
FROM distinct_invoices

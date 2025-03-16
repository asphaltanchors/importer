{{ config(materialized='table') }}

-- Diagnostic model to find the first occurrence of "6.625%"
WITH potential_issues AS (
  -- Check sales_receipts first
  SELECT
    'sales_receipts' as source_table,
    sr."Sales Receipt No" as document_number,
    sr."Product/Service" as product_code,
    'Product/Service Sales Tax Code' as field_name,
    sr."Product/Service Sales Tax Code" as field_value
  FROM {{ source('raw', 'sales_receipts') }} sr
  WHERE sr."Product/Service Sales Tax Code" = '6.625%'
  
  UNION ALL
  
  SELECT
    'sales_receipts' as source_table,
    sr."Sales Receipt No" as document_number,
    sr."Product/Service" as product_code,
    'Product/Service Rate' as field_name,
    sr."Product/Service Rate" as field_value
  FROM {{ source('raw', 'sales_receipts') }} sr
  WHERE sr."Product/Service Rate" = '6.625%'
  
  UNION ALL
  
  -- Check invoices next
  SELECT
    'invoices' as source_table,
    inv."Invoice No" as document_number,
    inv."Product/Service" as product_code,
    'Product/Service Sales Tax' as field_name,
    inv."Product/Service Sales Tax" as field_value
  FROM {{ source('raw', 'invoices') }} inv
  WHERE inv."Product/Service Sales Tax" = '6.625%'
  
  UNION ALL
  
  SELECT
    'invoices' as source_table,
    inv."Invoice No" as document_number,
    inv."Product/Service" as product_code,
    'Product/Service Rate' as field_name,
    inv."Product/Service Rate" as field_value
  FROM {{ source('raw', 'invoices') }} inv
  WHERE inv."Product/Service Rate" = '6.625%'
)

-- Return just the first occurrence
SELECT * FROM potential_issues
LIMIT 1

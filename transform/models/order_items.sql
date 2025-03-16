{{ config(materialized='table') }}

-- Sales Receipt line items
WITH sales_receipt_items AS (
  SELECT
    o.quickbooks_id as order_id,
    sr."Sales Receipt No" as order_number,
    'sales_receipt' as order_type,
    sr."Product/Service" as product_code,
    sr."Product/Service Description" as product_description,
    CAST(NULLIF(sr."Product/Service Quantity", '') AS NUMERIC) as quantity,
    CAST(NULLIF(sr."Product/Service Rate", '') AS NUMERIC) as unit_price,
    CAST(NULLIF(sr."Product/Service Amount", '') AS NUMERIC) as line_amount,
    sr."Product/Service Class" as product_class,
    sr."Product/Service Service Date" as service_date,
    sr."Product/Service Sales Tax Code" as sales_tax_code
  FROM {{ source('raw', 'sales_receipts') }} sr
  JOIN {{ ref('orders') }} o
    ON sr."Sales Receipt No" = o.order_number
    AND o.order_type = 'sales_receipt'
  WHERE sr."Product/Service" IS NOT NULL
    AND sr."Product/Service" != ''
    AND sr."Product/Service Rate" NOT LIKE '%\%%'  -- Skip records with % symbol
),

-- Invoice line items
invoice_items AS (
  SELECT
    o.quickbooks_id as order_id,
    inv."Invoice No" as order_number,
    'invoice' as order_type,
    inv."Product/Service" as product_code,
    inv."Product/Service Description" as product_description,
    CAST(NULLIF(inv."Product/Service Quantity", '') AS NUMERIC) as quantity,
    CAST(NULLIF(inv."Product/Service Rate", '') AS NUMERIC) as unit_price,
    CAST(NULLIF(inv."Product/Service  Amount", '') AS NUMERIC) as line_amount,
    inv."Product/Service Class" as product_class,
    inv."Product/Service Service Date" as service_date,
    inv."Product/Service Sales Tax" as sales_tax_code
  FROM {{ source('raw', 'invoices') }} inv
  JOIN {{ ref('orders') }} o
    ON inv."Invoice No" = o.order_number
    AND o.order_type = 'invoice'
  WHERE inv."Product/Service" IS NOT NULL
    AND inv."Product/Service" != ''
    AND inv."Product/Service Rate" NOT LIKE '%\%%'  -- Skip records with % symbol
)

-- Combine both sources
SELECT * FROM sales_receipt_items
UNION ALL
SELECT * FROM invoice_items

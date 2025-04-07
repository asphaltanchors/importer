{{ config(materialized='table') }}

-- Invoices data
WITH invoices AS (
  SELECT
    "QuickBooks Internal Id" as quickbooks_id,
    "Invoice No" as order_number,
    'invoice' as order_type,
    "Class" as class,
    "Terms" as terms,
    "Status" as status,
    "PO Number" as po_number,
    'Invoice' as payment_method,
    TO_DATE("Invoice Date", 'MM-DD-YYYY') as order_date,
    MAX(CAST(NULLIF("Total Amount", '') AS NUMERIC)) as total_amount,  -- Handle empty strings and cast to NUMERIC
    "Customer" as customer_name,
    "Billing Address Line1" as billing_address_line_1,
    "Billing Address Line2" as billing_address_line_2,
    "Billing Address Line3" as billing_address_line_3,
    "Billing Address City" as billing_address_city,
    "Billing Address State" as billing_address_state,
    "Billing Address Postal Code" as billing_address_postal_code,
    "Billing Address Country" as billing_address_country,
    "Shipping Address Line1" as shipping_address_line_1,
    "Shipping Address Line2" as shipping_address_line_2,
    "Shipping Address Line3" as shipping_address_line_3,
    "Shipping Address City" as shipping_address_city,
    "Shipping Address State" as shipping_address_state,
    "Shipping Address Postal Code" as shipping_address_postal_code,
    "Shipping Address Country" as shipping_address_country,
    NULL as industry,
    NULL as sourcechannel
  FROM {{ source('raw', 'invoices') }}
  WHERE "QuickBooks Internal Id" != ''
  GROUP BY
    "QuickBooks Internal Id",
    "Invoice No",
    "Class",
    "Terms",
    "Status",
    "PO Number",
    "Invoice Date",
    "Customer",
    "Billing Address Line1",
    "Billing Address Line2",
    "Billing Address Line3",
    "Billing Address City",
    "Billing Address State",
    "Billing Address Postal Code",
    "Billing Address Country",
    "Shipping Address Line1",
    "Shipping Address Line2",
    "Shipping Address Line3",
    "Shipping Address City",
    "Shipping Address State",
    "Shipping Address Postal Code",
    "Shipping Address Country"
),

-- Sales Receipts data
sales_receipts AS (
  SELECT
    "QuickBooks Internal Id" as quickbooks_id,
    "Sales Receipt No" as order_number,  
    'sales_receipt' as order_type,
    "Class" as class,
    NULL as terms,
    'Closed' as status,
    NULL as po_number,
    "Payment Method" as payment_method,
    TO_DATE("Sales Receipt Date", 'MM-DD-YYYY') as order_date,
    MAX(CAST(NULLIF("Total Amount", '') AS NUMERIC)) as total_amount,
    "Customer" as customer_name,
    "Billing Address Line 1" as billing_address_line_1,
    "Billing Address Line 2" as billing_address_line_2,
    "Billing Address Line 3" as billing_address_line_3,
    "Billing Address City" as billing_address_city,
    "Billing Address State" as billing_address_state,
    "Billing Address Postal Code" as billing_address_postal_code,
    "Billing Address Country" as billing_address_country,
    "Shipping Address Line 1" as shipping_address_line_1,
    "Shipping Address Line 2" as shipping_address_line_2,
    "Shipping Address Line 3" as shipping_address_line_3,
    "Shipping Address City" as shipping_address_city,
    "Shipping Address State" as shipping_address_state,
    "Shipping Address Postal Code" as shipping_address_postal_code,
    "Shipping Address Country" as shipping_address_country,
    "Industry" as industry,
    "SourceChannel" as sourcechannel
  FROM {{ source('raw', 'sales_receipts') }}
  WHERE "QuickBooks Internal Id" != ''
  GROUP BY
    "QuickBooks Internal Id",
    "Sales Receipt No",  
    "Sales Receipt Date",
    "Class",
    "Payment Method",
    "Customer",
    "Billing Address Line 1",
    "Billing Address Line 2",
    "Billing Address Line 3",
    "Billing Address City",
    "Billing Address State",
    "Billing Address Postal Code",
    "Billing Address Country",
    "Shipping Address Line 1",
    "Shipping Address Line 2",
    "Shipping Address Line 3",
    "Shipping Address City",
    "Shipping Address State",
    "Shipping Address Postal Code",
    "Shipping Address Country",
    "Industry",
    "SourceChannel"
)

-- Combine both sources
SELECT * FROM invoices
UNION ALL
SELECT * FROM sales_receipts

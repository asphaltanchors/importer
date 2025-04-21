SELECT 
    "Sales Receipt No" as sales_receipt_no,
    "Product/Service" as product_service,
    "Product/Service Description" as product_service_description,
    "QuickBooks Internal Id" as quickbooks_id,
    -- Other relevant fields
    _sdc_extracted_at,
    _sdc_batched_at
FROM {{ source('raw', 'sales_receipts') }}

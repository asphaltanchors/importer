SELECT 
    "Invoice No" as invoice_no,
    "Product/Service" as product_service,
    "QuickBooks Internal Id" as quickbooks_id,
    "Status" as status,
    -- Other relevant fields
    _sdc_extracted_at,
    _sdc_batched_at
FROM {{ source('raw', 'invoices') }}

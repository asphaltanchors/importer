SELECT 
    "QuickBooks Internal Id" as quickbooks_id,
    "Customer Name" as customer_name,
    "Company Name" as company_name,
    "First Name" as first_name,
    "Last Name" as last_name,
    "Customer Type" as customer_type,
    "Billing Address City" as billing_city,
    "Billing Address State" as billing_state,
    "Billing Address Postal Code" as billing_zip,
    "Shipping Address City" as shipping_city,
    "Shipping Address State" as shipping_state,
    "Shipping Address Postal Code" as shipping_zip,
    "Main Email" as email,
    "Status" as status,
    "Current Balance" as current_balance,
    _sdc_extracted_at,
    _sdc_batched_at
FROM {{ source('raw', 'customers') }}

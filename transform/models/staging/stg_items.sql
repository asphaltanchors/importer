SELECT 
    "Item Name" as item_name,
    "Purchase Cost" as purchase_cost,
    "Sales Price" as sales_price,
    "Sales Description" as sales_description,
    "Quantity On Hand" as quantity_on_hand,
    "Status" as status,
    _sdc_extracted_at,
    _sdc_batched_at
FROM {{ source('raw', 'items') }}

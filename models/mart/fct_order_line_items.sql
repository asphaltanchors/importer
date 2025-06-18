/*
  This fact table model provides order line items for frontend invoice recreation.
  Each row represents a single line item on an order/invoice, with complete order context
  and enriched product information for displaying customer invoices.
*/

{{ config(
    materialized = 'table',
    tags = ['orders', 'line_items', 'quickbooks']
) }}

WITH order_items AS (
    SELECT * FROM {{ ref('stg_quickbooks__order_items_tax_status') }}
),

-- Use staging data directly to avoid type casting issues in intermediate model
typed_order_items AS (
    SELECT
        -- Line item identifier
        _dlt_id as line_item_id,
        
        -- Order identifiers and metadata
        order_number,
        source_type,
        CASE 
            WHEN order_date IS NULL OR TRIM(order_date) = '' THEN NULL
            ELSE CAST(order_date AS DATE)
        END AS order_date,
        customer,
        payment_method,
        
        -- Standardized status
        CASE 
            WHEN UPPER(TRIM(status)) IN ('PAID', 'COMPLETE', 'COMPLETED') THEN 'PAID'
            WHEN UPPER(TRIM(status)) IN ('OPEN', 'UNPAID', 'PENDING') THEN 'OPEN'
            WHEN UPPER(TRIM(status)) IN ('PARTIALLY PAID', 'PARTIAL') THEN 'PARTIALLY_PAID'
            WHEN UPPER(TRIM(status)) IN ('CANCELLED', 'CANCELED', 'VOID') THEN 'CANCELLED'
            WHEN UPPER(TRIM(status)) IN ('OVERDUE') THEN 'OVERDUE'
            ELSE UPPER(TRIM(status))
        END AS status,
        
        CASE 
            WHEN due_date IS NULL OR TRIM(due_date) = '' THEN NULL
            ELSE CAST(due_date AS DATE)
        END AS due_date,
        
        -- Product/service information
        product_service,
        product_service_description,
        CAST(NULLIF(TRIM(product_service_quantity), '') AS NUMERIC) AS product_service_quantity,
        -- Clean rate field to handle percentage values and other non-numeric data
        CASE 
            WHEN NULLIF(TRIM(product_service_rate), '') IS NULL THEN NULL
            WHEN TRIM(product_service_rate) LIKE '%-%' AND TRIM(product_service_rate) LIKE '%' THEN NULL  -- Skip discount percentages
            WHEN TRIM(product_service_rate) ~ '^-?[0-9]+(\.[0-9]+)?$' THEN CAST(TRIM(product_service_rate) AS NUMERIC)
            ELSE NULL
        END AS product_service_rate,
        -- Clean amount field to handle percentage values and other non-numeric data
        CASE 
            WHEN NULLIF(TRIM(product_service_amount), '') IS NULL THEN NULL
            WHEN TRIM(product_service_amount) LIKE '%-%' AND TRIM(product_service_amount) LIKE '%' THEN NULL  -- Skip discount percentages
            WHEN TRIM(product_service_amount) ~ '^-?[0-9]+(\.[0-9]+)?$' THEN CAST(TRIM(product_service_amount) AS NUMERIC)
            ELSE NULL
        END AS product_service_amount,
        product_service_class,
        unit_of_measure,
        
        -- Tax information
        customer_sales_tax_code,
        is_tax_exempt,
        product_service_sales_tax_code,
        
        -- Address information (formatted for display)
        CONCAT_WS(', ',
            NULLIF(billing_address_line_1, ''),
            NULLIF(billing_address_line_2, ''),
            NULLIF(billing_address_line_3, '')
        ) AS billing_address,
        billing_address_city,
        billing_address_state,
        billing_address_postal_code,
        billing_address_country,
        
        CONCAT_WS(', ',
            NULLIF(shipping_address_line_1, ''),
            NULLIF(shipping_address_line_2, ''),
            NULLIF(shipping_address_line_3, '')
        ) AS shipping_address,
        shipping_address_city,
        shipping_address_state,
        shipping_address_postal_code,
        shipping_address_country,
        
        -- Shipping information
        shipping_method,
        CASE 
            WHEN ship_date IS NULL OR TRIM(ship_date) = '' THEN NULL
            ELSE CAST(ship_date AS DATE)
        END AS ship_date,
        
        -- Order details
        memo,
        message_to_customer,
        terms,
        sales_rep,
        class,
        
        -- Service dates and inventory
        product_service_service_date,
        inventory_site,
        inventory_bin,
        serial_no,
        lot_no,
        
        -- Other fields that might be useful for invoice display
        external_id,
        quick_books_internal_id,
        currency,
        CAST(NULLIF(TRIM(exchange_rate), '') AS NUMERIC) AS exchange_rate,
        
        -- Metadata
        CASE 
            WHEN created_date IS NULL OR TRIM(created_date) = '' THEN NULL
            ELSE CAST(created_date AS TIMESTAMP)
        END AS created_date,
        CASE 
            WHEN modified_date IS NULL OR TRIM(modified_date) = '' THEN NULL
            ELSE CAST(modified_date AS TIMESTAMP)
        END AS modified_date
        
    FROM order_items
),

-- Filter to actual line items (exclude description-only rows and empty product rows)
filtered_line_items AS (
    SELECT *
    FROM typed_order_items
    WHERE order_number IS NOT NULL
    AND TRIM(order_number) != ''
    AND product_service_amount IS NOT NULL
    AND (
        -- Include rows with actual products/services
        (NULLIF(TRIM(product_service), '') IS NOT NULL)
        OR 
        -- Include shipping and other service lines that have amounts
        (NULLIF(TRIM(product_service_description), '') IS NOT NULL AND product_service_amount != 0)
    )
),

-- Join with products for enrichment
enriched_line_items AS (
    SELECT
        li.*,
        
        -- Product enrichment from fct_products
        p.product_family,
        p.material_type,
        p.is_kit,
        p.item_type,
        p.item_subtype,
        p.sales_description as product_sales_description,
        p.sales_price as standard_sales_price,
        p.purchase_cost as standard_purchase_cost,
        p.margin_percentage,
        p.margin_amount
        
    FROM filtered_line_items li
    LEFT JOIN {{ ref('fct_products') }} p
        ON li.product_service = p.item_name
)

SELECT * FROM enriched_line_items
/*
  This intermediate model handles type casting and data quality issues for order items.
  It ensures all fields have the correct data types before aggregation and calculations.
*/

WITH order_items AS (
    SELECT * FROM {{ ref('stg_quickbooks__order_items_tax_status') }}
),

typed_order_items AS (
    SELECT
        -- Order identifiers
        order_number,
        source_type,
        transx AS transaction_id,
        quick_books_internal_id AS quickbooks_internal_id,
        
        -- Order metadata
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
            ELSE UPPER(TRIM(status))  -- Fallback to uppercase original value
        END AS status,
        CASE 
            WHEN due_date IS NULL OR TRIM(due_date) = '' THEN NULL
            ELSE CAST(due_date AS DATE)
        END AS due_date,
        
        -- Tax information
        customer_sales_tax_code,
        is_tax_exempt,
        CAST(NULLIF(TRIM(total_tax), '') AS NUMERIC) AS total_tax,
        CAST(NULLIF(REPLACE(TRIM(tax_persentage), '%', ''), '') AS NUMERIC) AS tax_percentage,
        
        -- Address information (raw)
        billing_address_line_1,
        billing_address_line_2,
        billing_address_line_3,
        billing_address_city,
        billing_address_state,
        billing_address_postal_code,
        billing_address_country AS billing_address_country_raw,
        
        shipping_address_line_1,
        shipping_address_line_2,
        shipping_address_line_3,
        shipping_address_city,
        shipping_address_state,
        shipping_address_postal_code,
        shipping_address_country AS shipping_address_country_raw,
        
        -- Normalized countries using macros
        {{ normalize_billing_country('billing_address_country', 'billing_address_state') }} AS billing_address_country,
        {{ normalize_shipping_country('shipping_address_country', 'shipping_address_state') }} AS shipping_address_country,
        
        -- Primary country using macro
        {{ normalize_country('billing_address_country', 'billing_address_state', 'shipping_address_country', 'shipping_address_state') }} AS primary_country,
        
        -- Country category using macro
        {{ country_category('billing_address_country', 'billing_address_state', 'shipping_address_country', 'shipping_address_state') }} AS country_category,
        
        -- Region grouping using macro
        {{ region('billing_address_country', 'billing_address_state', 'shipping_address_country', 'shipping_address_state') }} AS region,
        
        -- Shipping information
        shipping_method,
        CASE 
            WHEN ship_date IS NULL OR TRIM(ship_date) = '' THEN NULL
            ELSE CAST(ship_date AS DATE)
        END AS ship_date,
        
        -- Order metadata
        memo,
        message_to_customer,
        class,
        currency,
        CAST(NULLIF(TRIM(exchange_rate), '') AS NUMERIC) AS exchange_rate,
        terms,
        sales_rep,
        fob,
        
        -- Order flags
        CAST(print_later AS BOOLEAN) AS print_later,
        CAST(email_later AS BOOLEAN) AS email_later,
        external_id,
        CAST(is_pending AS BOOLEAN) AS is_pending,
        
        -- Dates
        CASE 
            WHEN created_date IS NULL OR TRIM(created_date) = '' THEN NULL
            ELSE CAST(created_date AS TIMESTAMP)
        END AS created_date,
        CASE 
            WHEN modified_date IS NULL OR TRIM(modified_date) = '' THEN NULL
            ELSE CAST(modified_date AS TIMESTAMP)
        END AS modified_date,
        
        -- Financial amounts (with robust numeric parsing)
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
        CAST(NULLIF(TRIM(total_amount), '') AS NUMERIC) AS total_amount,
        
        -- Other fields we want to preserve
        product_service,
        product_service_description,
        product_service_service_date,
        product_service_class,
        product_service_sales_tax_code,
        inventory_site,
        inventory_bin,
        unit_of_measure,
        serial_no,
        lot_no,
        other,
        other_1,
        other_2,
        template,
        industry,
        price_level,
        source_channel,
        unit_weight_kg,
        upc,
        load_date,
        _dlt_load_id,
        _dlt_id
    FROM order_items
),

-- Filter out rows with null critical values to ensure data quality
filtered_order_items AS (
    SELECT *
    FROM typed_order_items
    WHERE order_number IS NOT NULL
    AND TRIM(order_number) != ''
    AND product_service_amount IS NOT NULL 
    -- Removed total_tax and total_amount filters since they're order-level, not line-level
)

SELECT * FROM filtered_order_items
/*
  This fact table model provides order line items for frontend invoice recreation.
  Each row represents a single line item on an order/invoice, with complete order context
  and enriched product information for displaying customer invoices.
*/

{{ config(
    materialized = 'table',
    tags = ['orders', 'line_items', 'quickbooks']
) }}

WITH typed_order_items AS (
    SELECT * FROM {{ ref('int_quickbooks__order_items_typed') }}
),

-- Add the line-item specific fields for invoice display
enriched_order_items AS (
    SELECT
        -- Line item identifier
        _dlt_id as line_item_id,
        
        -- Order identifiers and metadata (already typed in intermediate)
        order_number,
        source_type,
        order_date,
        customer,
        payment_method,
        status,
        due_date,
        
        -- Product/service information (already typed in intermediate)
        product_service,
        product_service_description,
        product_service_quantity,
        product_service_rate,
        product_service_amount,
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
        
        -- Country fields for reporting (from intermediate model)
        primary_country,
        country_category,
        region,
        
        -- Shipping information
        shipping_method,
        ship_date,
        
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
        quickbooks_internal_id,
        currency,
        exchange_rate,
        
        -- Metadata
        created_date,
        modified_date
        
    FROM typed_order_items
),

-- Filter to actual line items (exclude description-only rows and empty product rows)
filtered_line_items AS (
    SELECT *
    FROM enriched_order_items
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
final_line_items AS (
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

SELECT * FROM final_line_items
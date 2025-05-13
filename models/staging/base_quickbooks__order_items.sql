{# Define sources to union #}
{% set sources = [
    {'source': 'raw_data', 'table': 'invoices', 'type': 'invoice'},
    {'source': 'raw_data', 'table': 'sales_receipts', 'type': 'sales_receipt'}
] %}

WITH unioned AS (
    {% for src in sources %}
    SELECT
        -- Common identifier
        {% if src.type == 'invoice' %}
            invoice_no as order_number,
        {% else %}
            sales_receipt_no as order_number,
        {% endif %}
        
        -- Customer information
        customer,
        
        -- Order date
        {% if src.type == 'invoice' %}
            invoice_date as order_date,
        {% else %}
            sales_receipt_date as order_date,
        {% endif %}
        
        -- Payment method
        {% if src.type == 'invoice' %}
            'invoice' as payment_method,
        {% else %}
            payment_method,
        {% endif %}
        
        -- Status
        {% if src.type == 'invoice' %}
            status,
        {% else %}
            'PAID' as status,
        {% endif %}
        
        -- Product information
        product_service,
        product_service_description,
        product_service_quantity,
        product_service_rate,
        product_service_amount,
        
        -- Service date
        product_service_service_date,
        
        -- Class
        product_service_class,
        
        -- Sales tax code
        {% if src.type == 'invoice' %}
            product_service_sales_tax as product_service_sales_tax_code,
        {% else %}
            product_service_sales_tax_code,
        {% endif %}
        
        -- Inventory information
        inventory_site,
        inventory_bin,
        unit_of_measure,
        
        -- Serial and lot numbers
        {% if src.type == 'invoice' %}
            serial_no,
            lot_no,
        {% else %}
            NULL as serial_no,
            NULL as lot_no,
        {% endif %}
        
        -- Billing address
        {% if src.type == 'invoice' %}
            billing_address_line1 as billing_address_line_1,
            billing_address_line2 as billing_address_line_2,
            billing_address_line3 as billing_address_line_3,
        {% else %}
            billing_address_line_1,
            billing_address_line_2,
            billing_address_line_3,
        {% endif %}
        billing_address_city,
        billing_address_state,
        billing_address_postal_code,
        billing_address_country,
        
        -- Shipping address
        {% if src.type == 'invoice' %}
            shipping_address_line1 as shipping_address_line_1,
            shipping_address_line2 as shipping_address_line_2,
            shipping_address_line3 as shipping_address_line_3,
        {% else %}
            shipping_address_line_1,
            shipping_address_line_2,
            shipping_address_line_3,
        {% endif %}
        shipping_address_city,
        shipping_address_state,
        shipping_address_postal_code,
        shipping_address_country,
        
        -- Shipping information
        shipping_method,
        ship_date,
        
        -- Tax information
        customer_sales_tax_code,
        {% if src.type == 'invoice' %}
            sales_tax as sales_tax_item,
        {% else %}
            sales_tax_item,
        {% endif %}
        total_tax,
        {% if src.type == 'invoice' %}
            tax_persentage,
        {% else %}
            NULL as tax_persentage,
        {% endif %}
        
        -- Dates
        due_date,
        
        -- Notes and messages
        memo,
        {% if src.type == 'invoice' %}
            customer_message as message_to_customer,
        {% else %}
            message_to_customer,
        {% endif %}
        
        -- Classification
        class,
        
        -- Currency
        currency,
        exchange_rate,
        
        -- Terms
        {% if src.type == 'invoice' %}
            terms,
        {% else %}
            NULL as terms,
        {% endif %}
        
        -- Sales rep
        sales_rep,
        
        -- FOB
        fob,
        
        -- Print and email flags
        print_later,
        email_later,
        
        -- Other fields
        other,
        other_1,
        other_2,
        template,
        
        -- External ID
        {% if src.type == 'invoice' %}
            external_id,
        {% else %}
            NULL as external_id,
        {% endif %}
        
        -- Is pending (sales receipts only)
        {% if src.type == 'invoice' %}
            NULL as is_pending,
        {% else %}
            is_pending,
        {% endif %}
        
        -- Total amount
        total_amount,
        
        -- Dates
        created_date,
        modified_date,
        
        -- Transaction ID
        transx,
        
        -- QuickBooks internal ID
        quick_books_internal_id,
        
        -- Additional fields
        industry,
        price_level,
        source_channel,
        unit_weight_kg,
        upc,
        load_date,
        
        -- DLT fields
        _dlt_load_id,
        _dlt_id,
        
        -- Source type
        '{{ src.type }}' as source_type
        
    FROM {{ source(src.source, src.table) }}
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

SELECT * FROM unioned

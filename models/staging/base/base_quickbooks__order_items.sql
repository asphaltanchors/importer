{# Define sources to union #}
{% set sources = [
    {'source': 'raw_data', 'table': 'xlsx_invoice', 'type': 'invoice'},
    {'source': 'raw_data', 'table': 'xlsx_sales_receipt', 'type': 'sales_receipt'}
] %}

WITH unioned AS (
    {% for src in sources %}
    SELECT
        -- Order identifiers
        {% if src.type == 'invoice' %}
            invoice_no as order_number,
            invoice_date as order_date,
            'invoice' as payment_method,
            status,
            product_service__amount as product_service_amount,
            transxx as transx,
        {% else %}
            sales_receipt_no as order_number,
            sales_receipt_date as order_date,
            payment_method,
            'PAID' as status,
            product_service_amount,
            NULL::bigint as transx,
        {% endif %}
        
        -- Common core fields
        customer,
        product_service,
        product_service_description,
        product_service_quantity,
        product_service_rate,
        customer_sales_tax_code,
        
        -- Date fields
        {% if src.type == 'invoice' %}
            due_date,
            ship_date,
        {% else %}
            due_date,
            ship_date,
        {% endif %}
        
        -- Tax fields (use correct numeric tax fields)
        {% if src.type == 'invoice' %}
            total_tax,  -- Use the numeric total_tax field, not the text sales_tax field
            tax_persentage,
            total_amount,
            sales_tax as sales_tax_code,  -- Preserve the text tax code for reference
        {% else %}
            total_tax,
            NULL::double precision as tax_persentage,
            total_amount,
            NULL::text as sales_tax_code,
        {% endif %}
        
        -- Address fields
        {% if src.type == 'invoice' %}
            billing_address_line1 as billing_address_line_1,
            billing_address_line2 as billing_address_line_2,
            billing_address_line3 as billing_address_line_3,
            billing_address_city,
            billing_address_state,
            billing_address_postal_code,
            billing_address_country,
            shipping_address_line1 as shipping_address_line_1,
            shipping_address_line2 as shipping_address_line_2,
            shipping_address_line3 as shipping_address_line_3,
            shipping_address_city,
            shipping_address_state,
            shipping_address_postal_code,
            shipping_address_country,
        {% else %}
            billing_address_line_1,
            billing_address_line_2,
            billing_address_line_3,
            billing_address_city,
            billing_address_state,
            billing_address_postal_code,
            NULL as billing_address_country,
            shipping_address_line_1,
            shipping_address_line_2,
            shipping_address_line_3,
            shipping_address_city,
            shipping_address_state,
            shipping_address_postal_code,
            NULL as shipping_address_country,
        {% endif %}
        
        -- Other fields (minimal set that works)
        shipping_method,
        
        -- Date fields
        created_date,
        modified_date,
        
        -- Flags
        print_later,
        email_later,
        {% if src.type == 'invoice' %}
            NULL::boolean as is_pending,
        {% else %}
            is_pending,
        {% endif %}
        
        -- Additional fields from XLSX
        class,
        
        -- Additional fields available in XLSX data
        memo,
        {% if src.type == 'invoice' %}
            customer_message as message_to_customer,
        {% else %}
            message_to_customer,
        {% endif %}
        currency,
        exchange_rate,
        {% if src.type == 'invoice' %}
            terms,
        {% else %}
            NULL as terms,
        {% endif %}
        sales_rep,
        fob,
        other,
        other_1,
        other_2,
        template,
        {% if src.type == 'invoice' %}
            external_id,
        {% else %}
            NULL as external_id,
        {% endif %}
        CAST(industry AS VARCHAR) as industry,
        CAST(price_level AS VARCHAR) as price_level,
        source_channel,
        CAST(unit_weight_kg AS VARCHAR) as unit_weight_kg,
        CAST(upc AS VARCHAR) as upc,
        product_service_service_date,
        product_service_class,
        {% if src.type == 'invoice' %}
            product_service_sales_tax as product_service_sales_tax_code,
        {% else %}
            product_service_sales_tax_code,
        {% endif %}
        inventory_site,
        inventory_bin,
        unit_of_measure,
        {% if src.type == 'invoice' %}
            serial_no,
            lot_no,
        {% else %}
            NULL as serial_no,
            NULL as lot_no,
        {% endif %}
        
        -- Metadata
        quick_books_internal_id,
        load_date,
        _dlt_load_id,
        _dlt_id,
        
        -- Source type
        '{{ src.type }}' as source_type
        
    FROM {{ source(src.source, src.table) }}
    WHERE total_amount IS NOT NULL  -- Filter out rows with invalid total_amount
    {% if not loop.last %}UNION ALL{% endif %}
    {% endfor %}
)

SELECT * FROM unioned
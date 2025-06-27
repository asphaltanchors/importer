/*
  This fact table model represents orders at the order level for business consumption.
  It builds on the intermediate model and adds additional business logic and metrics.
  
  Fact tables contain:
  - Business events/transactions
  - Foreign keys to dimensions
  - Business metrics/measures
*/

{{ config(
    materialized = 'table',
    tags = ['orders', 'quickbooks']
) }}

WITH orders AS (
    SELECT * FROM {{ ref('int_quickbooks__orders') }}
),

-- Add any additional transformations or business logic here
orders_enriched AS (
    SELECT
        -- Primary key
        order_number,
        
        -- Order metadata
        source_type,
        order_date,
        customer,
        payment_method,
        status,
        due_date,
        
        -- Flag fields
        is_tax_exempt,
        
        -- Additional flags/calculations
        CASE 
            WHEN status = 'PAID' THEN TRUE
            ELSE FALSE
        END AS is_paid,
        
        CASE
            WHEN due_date IS NOT NULL AND order_date IS NOT NULL AND due_date < order_date THEN TRUE
            ELSE FALSE
        END AS is_backdated,
        
        -- Addresses
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
        
        -- Country fields for reporting
        primary_country,
        country_category,
        region,
        
        -- Shipping information
        shipping_method,
        ship_date,
        
        -- Order details
        memo,
        message_to_customer,
        class,
        currency,
        exchange_rate,
        terms,
        sales_rep,
        
        -- Identifiers for joins
        transaction_id,
        quickbooks_internal_id,
        external_id,
        
        -- Dates for analytics
        created_date,
        modified_date,
        
        -- Metrics
        COALESCE(total_line_items_amount, 0.0) as total_line_items_amount,
        COALESCE(total_tax, 0.0) as total_tax,
        COALESCE(total_amount, 0.0) as total_amount,
        COALESCE(item_count, 0) as item_count,
        
        -- Derived metrics
        CASE 
            WHEN COALESCE(total_tax, 0) = 0 OR COALESCE(total_amount, 0) = 0 THEN 0
            ELSE ROUND(CAST((total_tax / total_amount) * 100 AS NUMERIC), 2)
        END AS effective_tax_rate
    FROM orders
)

SELECT * FROM orders_enriched
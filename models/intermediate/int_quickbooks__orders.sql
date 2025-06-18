/*
  This intermediate model aggregates order items to the order level.
  It performs initial grouping of line items while preserving all order-level information.
  
  For boolean fields, we use BOOL_OR which returns TRUE if any of the values in the group are TRUE.
  This means a flag is considered TRUE for the entire order if it's TRUE for any line item.
*/

WITH order_items AS (
    SELECT * FROM {{ ref('int_quickbooks__order_items_typed') }}
),

-- Group by order_number to get order-level data
aggregated_orders AS (
    SELECT
        -- Order identifiers
        order_number,
        MAX(source_type) AS source_type,
        MAX(transaction_id) AS transaction_id,
        MAX(quickbooks_internal_id) AS quickbooks_internal_id,
        
        -- Order metadata
        MAX(order_date) AS order_date,
        MAX(customer) AS customer,
        MAX(payment_method) AS payment_method,
        MAX(status) AS status,
        MAX(due_date) AS due_date,
        
        -- Tax information
        MAX(customer_sales_tax_code) AS customer_sales_tax_code,
        BOOL_OR(is_tax_exempt) AS is_tax_exempt,  -- Order is considered tax exempt if any items are tax exempt
        MAX(total_tax) AS total_tax,  -- Use MAX instead of SUM since total_tax is order-level like total_amount
        MAX(tax_percentage) AS tax_percentage,
        
        -- Address information
        MAX(billing_address_line_1) AS billing_address_line_1,
        MAX(billing_address_line_2) AS billing_address_line_2,
        MAX(billing_address_line_3) AS billing_address_line_3,
        MAX(billing_address_city) AS billing_address_city,
        MAX(billing_address_state) AS billing_address_state,
        MAX(billing_address_postal_code) AS billing_address_postal_code,
        MAX(billing_address_country) AS billing_address_country,
        
        MAX(shipping_address_line_1) AS shipping_address_line_1,
        MAX(shipping_address_line_2) AS shipping_address_line_2,
        MAX(shipping_address_line_3) AS shipping_address_line_3,
        MAX(shipping_address_city) AS shipping_address_city,
        MAX(shipping_address_state) AS shipping_address_state,
        MAX(shipping_address_postal_code) AS shipping_address_postal_code,
        MAX(shipping_address_country) AS shipping_address_country,
        
        -- Shipping information
        MAX(shipping_method) AS shipping_method,
        MAX(ship_date) AS ship_date,
        
        -- Order metadata
        MAX(memo) AS memo,
        MAX(message_to_customer) AS message_to_customer,
        MAX(class) AS class,
        MAX(currency) AS currency,
        MAX(exchange_rate) AS exchange_rate,
        MAX(terms) AS terms,
        MAX(sales_rep) AS sales_rep,
        MAX(fob) AS fob,
        
        -- Order flags (using BOOL_OR to aggregate boolean fields)
        BOOL_OR(print_later) AS print_later,      -- TRUE if any items have print_later flag
        BOOL_OR(email_later) AS email_later,      -- TRUE if any items have email_later flag
        MAX(external_id) AS external_id,
        BOOL_OR(is_pending) AS is_pending,        -- TRUE if any items are pending
        
        -- Dates
        MAX(created_date) AS created_date,
        MAX(modified_date) AS modified_date,
        
        -- Aggregated measures
        SUM(product_service_amount) AS total_line_items_amount,
        MAX(total_amount) AS total_amount,  -- Use MAX instead of SUM since total_amount is order-level, not line-level
        COUNT(*) AS item_count
        
    FROM order_items
    GROUP BY order_number
),

-- Filter out orders with null critical fields
filtered_orders AS (
    SELECT *
    FROM aggregated_orders
    WHERE order_date IS NOT NULL
    AND total_amount IS NOT NULL
    AND order_number IS NOT NULL
    AND TRIM(order_number) != ''  -- Also filter out empty order numbers
)

SELECT * FROM filtered_orders
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
        MAX(billing_address_country) AS billing_address_country_raw,
        
        MAX(shipping_address_line_1) AS shipping_address_line_1,
        MAX(shipping_address_line_2) AS shipping_address_line_2,
        MAX(shipping_address_line_3) AS shipping_address_line_3,
        MAX(shipping_address_city) AS shipping_address_city,
        MAX(shipping_address_state) AS shipping_address_state,
        MAX(shipping_address_postal_code) AS shipping_address_postal_code,
        MAX(shipping_address_country) AS shipping_address_country_raw,
        
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

-- Add country normalization to orders
orders_with_countries AS (
    SELECT 
        *,
        
        -- Normalized countries using macros
        {{ normalize_billing_country('billing_address_country_raw', 'billing_address_state') }} AS billing_address_country,
        {{ normalize_shipping_country('shipping_address_country_raw', 'shipping_address_state') }} AS shipping_address_country,
        
        -- Primary country using macro
        {{ normalize_country('billing_address_country_raw', 'billing_address_state', 'shipping_address_country_raw', 'shipping_address_state') }} AS primary_country,
        
        -- Country category using macro
        {{ country_category('billing_address_country_raw', 'billing_address_state', 'shipping_address_country_raw', 'shipping_address_state') }} AS country_category,
        
        -- Region grouping using macro
        {{ region('billing_address_country_raw', 'billing_address_state', 'shipping_address_country_raw', 'shipping_address_state') }} AS region
        
    FROM aggregated_orders
),

-- Add sales channel and customer segment attribution
orders_with_attribution AS (
    SELECT 
        *,
        
        -- Sales Channel Attribution (HOW they bought)
        CASE 
            -- Amazon Channel (clear payment method or customer indicators)
            WHEN payment_method = 'Amazon' THEN 'Amazon'
            WHEN customer = 'Amazon FBA' THEN 'Amazon FBA'  
            WHEN class LIKE '%Amazon%' THEN 'Amazon'
            
            -- Website Channel (immediate payment via credit/digital, no invoice terms)
            WHEN source_type = 'sales_receipt' 
             AND payment_method IN ('Credit Card', 'PayPal', 'Visa', 'MasterCard', 'American Express', 'Discover') 
             THEN 'Website'
            
            -- Phone/Direct Channel (invoices with credit card terms - phone orders that became invoices)
            WHEN source_type = 'invoice' 
             AND terms IN ('CC', 'Credit Card') 
             THEN 'Phone/Direct'
             
            -- Invoice/Terms Channel (traditional B2B invoice flow)  
            WHEN source_type = 'invoice' 
             AND terms IN ('Net 30', 'N30', 'Net 20', 'Prepaid', 'Prepaid TT') 
             THEN 'Invoice/ACH'
            
            ELSE 'Other'
        END AS sales_channel,
        
        -- Customer Segment Attribution (WHO they are)
        CASE 
            -- OEM (high-value B2B with special discount relationship)
            WHEN class = 'OEM' THEN 'OEM'
            
            -- Distributors (B2B resellers, primarily net terms)
            WHEN class = 'Distributor' THEN 'Distributor'
            
            -- Contractors (B2B project-based)
            WHEN class = 'Contractor' THEN 'Contractor'
            
            -- Export customers
            WHEN class IN ('EXPORT', 'EXPORT from WWD') THEN 'Export'
            
            -- Direct Consumers (sales receipts = immediate payment)
            WHEN source_type = 'sales_receipt' AND class NOT IN ('OEM', 'Distributor') THEN 'Direct Consumer'
            
            -- B2B Direct (invoices that aren't distributors/OEMs/contractors)
            WHEN source_type = 'invoice' AND class NOT IN ('OEM', 'Distributor', 'Contractor', 'EXPORT', 'EXPORT from WWD') THEN 'B2B Direct'
            
            ELSE 'Other'
        END AS customer_segment
        
    FROM orders_with_countries
),

-- Filter out orders with null critical fields
filtered_orders AS (
    SELECT *
    FROM orders_with_attribution
    WHERE order_date IS NOT NULL
    AND total_amount IS NOT NULL
    AND order_number IS NOT NULL
    AND TRIM(order_number) != ''  -- Also filter out empty order numbers
)

SELECT * FROM filtered_orders
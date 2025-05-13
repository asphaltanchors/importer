/*
  This model transforms the base QuickBooks order items data to add a tax exemption flag
  based on the customer_sales_tax_code field. It determines whether an order is tax exempt:
  - 'Tax' values are NOT exempt (false)
  - 'Non' values ARE exempt (true)
  - Other values will be handled accordingly with appropriate defaults
*/

WITH order_items_with_tax_status AS (
    SELECT
        *,
        -- Determine if order is tax exempt based on customer_sales_tax_code
        -- 'Tax' = false (not exempt)
        -- 'Non' = true (exempt)
        CASE 
            WHEN customer_sales_tax_code ILIKE '%non%' THEN TRUE
            WHEN customer_sales_tax_code ILIKE '%tax%' THEN FALSE
            ELSE NULL -- Handle unexpected values
        END AS is_tax_exempt
        
    FROM {{ ref('base_quickbooks__order_items') }}
)

SELECT
    -- Include key fields explicitly
    order_number,
    customer,
    order_date,
    payment_method,
    status,
    customer_sales_tax_code,
    is_tax_exempt, -- New tax exempt flag
    
    -- Include all remaining fields
    {{ dbt_utils.star(from=ref('base_quickbooks__order_items'), 
                     except=[
                       "order_number", "customer", "order_date", 
                       "payment_method", "status", "customer_sales_tax_code"
                     ]) }}
FROM order_items_with_tax_status
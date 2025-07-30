-- ABOUTME: Intermediate model aggregating customer-level revenue metrics from order data
-- ABOUTME: Combines invoice and sales receipt data to calculate total customer value and activity

{{ config(
    materialized = 'table',
    tags = ['intermediate', 'customers', 'revenue']
) }}

-- Get customer-level revenue from unified order items (invoices + sales receipts)
WITH customer_revenue_aggregated AS (
    SELECT 
        customer,
        SUM(product_service_amount) as customer_total_revenue,
        COUNT(DISTINCT order_number) as customer_total_orders,
        COUNT(*) as customer_total_line_items,
        MIN(CAST(order_date AS DATE)) as customer_first_order_date,
        MAX(CAST(order_date AS DATE)) as customer_latest_order_date,
        COUNT(DISTINCT CAST(order_date AS DATE)) as customer_order_days
    FROM {{ ref('base_quickbooks__order_items') }}
    WHERE product_service_amount IS NOT NULL 
      AND product_service_amount > 0
    GROUP BY customer
)

SELECT 
    customer,
    customer_total_revenue,
    customer_total_orders,
    customer_total_line_items,
    customer_first_order_date,
    customer_latest_order_date,
    customer_order_days,
    
    -- Customer value classification
    CASE 
        WHEN customer_total_revenue >= 50000 THEN 'High Value Customer ($50K+)'
        WHEN customer_total_revenue >= 10000 THEN 'Medium Value Customer ($10K-$50K)'
        WHEN customer_total_revenue >= 1000 THEN 'Regular Customer ($1K-$10K)'
        WHEN customer_total_revenue > 0 THEN 'Low Value Customer (<$1K)'
        ELSE 'No Revenue'
    END as customer_value_tier,
    
    -- Customer activity classification
    CASE 
        WHEN customer_latest_order_date >= CURRENT_DATE - INTERVAL '90 days' THEN 'Active (Last 90 Days)'
        WHEN customer_latest_order_date >= CURRENT_DATE - INTERVAL '1 year' THEN 'Recent (Last Year)'
        WHEN customer_latest_order_date >= CURRENT_DATE - INTERVAL '2 years' THEN 'Dormant (1-2 Years)'
        WHEN customer_latest_order_date IS NOT NULL THEN 'Inactive (2+ Years)'
        ELSE 'No Orders'
    END as customer_activity_status,
    
    -- Customer ordering frequency
    CASE 
        WHEN customer_order_days > 0 AND customer_total_orders > 0 THEN
            ROUND(CAST(customer_total_orders AS NUMERIC) / CAST(customer_order_days AS NUMERIC), 2)
        ELSE 0
    END as orders_per_day,
    
    -- Activity flags
    CASE WHEN customer_total_revenue > 0 THEN TRUE ELSE FALSE END as has_revenue,
    CASE WHEN customer_latest_order_date >= CURRENT_DATE - INTERVAL '1 year' THEN TRUE ELSE FALSE END as is_active_customer,
    
    -- Metadata
    CURRENT_TIMESTAMP as created_at

FROM customer_revenue_aggregated
ORDER BY customer_total_revenue DESC, customer
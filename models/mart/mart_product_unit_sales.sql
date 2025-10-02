/*
ABOUTME: This mart table provides aggregated unit sales data for frontend consumption and analytics.
ABOUTME: Enables queries like "5,234 UNITS of SP10 sold in last 30 days" by aggregating individual unit sales by product and time period.
*/

{{ config(
    materialized = 'table',
    tags = ['sales', 'units', 'products']
) }}

WITH order_line_items AS (
    SELECT
        product_service,
        packaging_type,
        units_per_sku,
        sku_quantity,
        total_units_sold,
        order_date,
        customer,
        source_type,
        product_service_amount,
        product_family,
        material_type,
        is_kit
    FROM {{ ref('fct_order_line_items') }}
    WHERE product_service IS NOT NULL
    AND TRIM(product_service) != ''
    AND order_date IS NOT NULL
    AND total_units_sold > 0
),

-- Aggregate unit sales by product and various time periods
product_unit_sales AS (
    SELECT
        product_service as product_name,

        -- Product characteristics
        MAX(packaging_type) as packaging_type,
        MAX(units_per_sku) as units_per_sku,
        MAX(product_family) as product_family,
        MAX(material_type) as material_type,
        BOOL_OR(is_kit) as is_kit,

        -- Time-based aggregations
        DATE_TRUNC('day', order_date) as sale_date,
        DATE_TRUNC('week', order_date) as sale_week,
        DATE_TRUNC('month', order_date) as sale_month,
        DATE_TRUNC('quarter', order_date) as sale_quarter,
        DATE_TRUNC('year', order_date) as sale_year,

        -- Sales metrics
        COUNT(*) as line_item_count,
        SUM(sku_quantity) as total_sku_quantity,
        SUM(total_units_sold) as total_units_sold,
        SUM(product_service_amount) as total_revenue,

        -- Customer metrics
        COUNT(DISTINCT customer) as unique_customers,

        -- Source type breakdown
        COUNT(CASE WHEN source_type = 'Sales Receipt' THEN 1 END) as sales_receipt_lines,
        COUNT(CASE WHEN source_type = 'Invoice' THEN 1 END) as invoice_lines,

        -- Unit metrics by source
        SUM(CASE WHEN source_type = 'Sales Receipt' THEN total_units_sold ELSE 0 END) as units_from_sales_receipts,
        SUM(CASE WHEN source_type = 'Invoice' THEN total_units_sold ELSE 0 END) as units_from_invoices

    FROM order_line_items
    GROUP BY
        product_service,
        DATE_TRUNC('day', order_date),
        DATE_TRUNC('week', order_date),
        DATE_TRUNC('month', order_date),
        DATE_TRUNC('quarter', order_date),
        DATE_TRUNC('year', order_date)
),

-- Add convenience fields for common date range queries
final_unit_sales AS (
    SELECT
        *,

        -- Convenience flags for common date filters
        CASE WHEN sale_date >= CURRENT_DATE - INTERVAL '30 days' THEN total_units_sold ELSE 0 END as units_last_30_days,
        CASE WHEN sale_date >= CURRENT_DATE - INTERVAL '90 days' THEN total_units_sold ELSE 0 END as units_last_90_days,
        CASE WHEN sale_date >= CURRENT_DATE - INTERVAL '1 year' THEN total_units_sold ELSE 0 END as units_last_year,

        -- Revenue per unit calculations
        CASE
            WHEN total_units_sold > 0
            THEN ROUND(CAST(total_revenue / total_units_sold AS NUMERIC), 4)
            ELSE 0
        END as revenue_per_unit,

        -- Metadata
        CURRENT_TIMESTAMP as created_at

    FROM product_unit_sales
)

SELECT * FROM final_unit_sales
ORDER BY sale_date DESC, total_units_sold DESC
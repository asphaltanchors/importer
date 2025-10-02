/*
  ABOUTME: Time-series margin analytics by product/SKU for dashboard consumption.
  ABOUTME: Provides aggregated actual vs standard margins with temporal dimensions.
*/

{{ config(
    materialized = 'table',
    tags = ['margins', 'analytics', 'products']
) }}

WITH line_items_with_margins AS (
    SELECT
        -- Product identifiers
        product_service as sku,
        product_family,
        material_type,
        is_kit,

        -- Time dimensions
        order_date,
        DATE_TRUNC('month', order_date) as order_month,
        DATE_TRUNC('quarter', order_date) as order_quarter,
        DATE_TRUNC('year', order_date) as order_year,

        -- Customer and order context
        customer,
        order_number,
        product_service_class,

        -- Quantity and pricing
        product_service_quantity as quantity_sold,
        total_units_sold,

        -- Standard pricing (from product catalog)
        standard_sales_price,
        standard_purchase_cost,
        margin_percentage as standard_margin_percentage,
        margin_amount as standard_margin_amount,

        -- Actual pricing (what was charged)
        actual_unit_price,
        actual_margin_amount,
        actual_margin_percentage,

        -- Discount analysis
        price_discount_amount,
        price_discount_percentage,

        -- Revenue calculations
        product_service_amount as line_total_revenue,
        CASE
            WHEN actual_margin_amount IS NOT NULL AND product_service_quantity IS NOT NULL
            THEN actual_margin_amount * product_service_quantity
            ELSE NULL
        END as line_total_margin_amount

    FROM {{ ref('fct_order_line_items') }}
    WHERE product_service IS NOT NULL
    AND TRIM(product_service) != ''
    AND order_date IS NOT NULL
    -- Exclude discount line items and other non-product entries
    AND actual_unit_price > 0
),

-- Daily aggregations by SKU
daily_margins AS (
    SELECT
        sku,
        order_date,
        product_family,
        material_type,
        is_kit,

        -- Volume metrics
        COUNT(*) as transaction_count,
        COUNT(DISTINCT customer) as unique_customers,
        COUNT(DISTINCT order_number) as unique_orders,
        SUM(quantity_sold) as total_quantity_sold,
        SUM(total_units_sold) as total_units_sold,

        -- Revenue metrics
        SUM(line_total_revenue) as total_revenue,
        AVG(actual_unit_price) as avg_unit_price,

        -- Standard margin metrics (for comparison)
        AVG(standard_sales_price) as avg_standard_price,
        AVG(standard_purchase_cost) as avg_purchase_cost,
        AVG(standard_margin_percentage) as avg_standard_margin_percentage,

        -- Actual margin metrics
        SUM(line_total_margin_amount) as total_margin_amount,
        AVG(actual_margin_amount) as avg_unit_margin_amount,
        AVG(actual_margin_percentage) as avg_margin_percentage,

        -- Volume-weighted margin percentage
        CASE
            WHEN SUM(line_total_revenue) > 0
            THEN ROUND(
                CAST((SUM(line_total_margin_amount) / SUM(line_total_revenue)) * 100 AS NUMERIC),
                2
            )
            ELSE NULL
        END as volume_weighted_margin_percentage,

        -- Discount analysis (volume-weighted for actual business impact)
        SUM(price_discount_amount * quantity_sold) as total_discount_amount,
        CASE
            WHEN SUM(standard_sales_price * quantity_sold) > 0
            THEN ROUND(
                CAST(
                    (1 - (SUM(actual_unit_price * quantity_sold) / SUM(standard_sales_price * quantity_sold))) * 100
                AS NUMERIC),
                2
            )
            ELSE NULL
        END as volume_weighted_discount_percentage

    FROM line_items_with_margins
    GROUP BY 1, 2, 3, 4, 5
),

-- Monthly aggregations by SKU
monthly_margins AS (
    SELECT
        sku,
        order_month,
        DATE_TRUNC('year', order_month) as order_year,
        product_family,
        material_type,
        is_kit,

        -- Volume metrics
        COUNT(*) as transaction_count,
        COUNT(DISTINCT customer) as unique_customers,
        COUNT(DISTINCT order_number) as unique_orders,
        SUM(quantity_sold) as total_quantity_sold,
        SUM(total_units_sold) as total_units_sold,

        -- Revenue metrics
        SUM(line_total_revenue) as total_revenue,
        AVG(actual_unit_price) as avg_unit_price,

        -- Standard margin metrics (for comparison)
        AVG(standard_sales_price) as avg_standard_price,
        AVG(standard_purchase_cost) as avg_purchase_cost,
        AVG(standard_margin_percentage) as avg_standard_margin_percentage,

        -- Actual margin metrics
        SUM(line_total_margin_amount) as total_margin_amount,
        AVG(actual_margin_amount) as avg_unit_margin_amount,
        AVG(actual_margin_percentage) as avg_margin_percentage,

        -- Volume-weighted margin percentage
        CASE
            WHEN SUM(line_total_revenue) > 0
            THEN ROUND(
                CAST((SUM(line_total_margin_amount) / SUM(line_total_revenue)) * 100 AS NUMERIC),
                2
            )
            ELSE NULL
        END as volume_weighted_margin_percentage,

        -- Discount analysis (volume-weighted for actual business impact)
        SUM(price_discount_amount * quantity_sold) as total_discount_amount,
        CASE
            WHEN SUM(standard_sales_price * quantity_sold) > 0
            THEN ROUND(
                CAST(
                    (1 - (SUM(actual_unit_price * quantity_sold) / SUM(standard_sales_price * quantity_sold))) * 100
                AS NUMERIC),
                2
            )
            ELSE NULL
        END as volume_weighted_discount_percentage

    FROM line_items_with_margins
    GROUP BY 1, 2, 3, 4, 5, 6
),

-- Final union with time period indicators
final_aggregations AS (
    SELECT
        'daily' as time_period,
        sku,
        order_date as period_date,
        order_date,
        DATE_TRUNC('month', order_date) as order_month,
        DATE_TRUNC('year', order_date) as order_year,
        product_family,
        material_type,
        is_kit,
        transaction_count,
        unique_customers,
        unique_orders,
        total_quantity_sold,
        total_units_sold,
        total_revenue,
        avg_unit_price,
        avg_standard_price,
        avg_purchase_cost,
        avg_standard_margin_percentage,
        total_margin_amount,
        avg_unit_margin_amount,
        avg_margin_percentage,
        volume_weighted_margin_percentage,
        total_discount_amount,
        volume_weighted_discount_percentage
    FROM daily_margins

    UNION ALL

    SELECT
        'monthly' as time_period,
        sku,
        order_month as period_date,
        NULL as order_date,
        order_month,
        order_year,
        product_family,
        material_type,
        is_kit,
        transaction_count,
        unique_customers,
        unique_orders,
        total_quantity_sold,
        total_units_sold,
        total_revenue,
        avg_unit_price,
        avg_standard_price,
        avg_purchase_cost,
        avg_standard_margin_percentage,
        total_margin_amount,
        avg_unit_margin_amount,
        avg_margin_percentage,
        volume_weighted_margin_percentage,
        total_discount_amount,
        volume_weighted_discount_percentage
    FROM monthly_margins
)

SELECT * FROM final_aggregations
ORDER BY sku, time_period, period_date DESC
/*
ABOUTME: This fact table captures historical pricing data derived from actual transactions.
ABOUTME: It provides daily pricing metrics for products based on invoices and sales receipts.
*/

{{ config(
    materialized = 'table',
    tags = ['pricing', 'products', 'history', 'quickbooks']
) }}

WITH transaction_data AS (
    -- Get all transaction line items with pricing data
    SELECT 
        product_service,
        order_date,
        source_type,
        product_service_rate,
        product_service_quantity,
        product_service_amount,
        load_date
    FROM {{ ref('int_quickbooks__order_items_typed') }}
    WHERE product_service IS NOT NULL 
        AND TRIM(product_service) != ''
        AND product_service_rate IS NOT NULL
        AND product_service_rate > 0
        AND order_date IS NOT NULL
        AND product_service_amount IS NOT NULL
        AND product_service_amount > 0
),

-- Aggregate pricing data by product and date
daily_pricing AS (
    SELECT
        product_service,
        order_date,
        
        -- Price metrics
        AVG(product_service_rate) AS avg_unit_price,
        MIN(product_service_rate) AS min_unit_price,
        MAX(product_service_rate) AS max_unit_price,
        
        -- Volume metrics
        SUM(COALESCE(product_service_quantity, 0)) AS total_units_sold,
        SUM(product_service_amount) AS total_revenue,
        COUNT(*) AS transaction_count,
        
        -- Price volatility
        CASE 
            WHEN COUNT(*) > 1 THEN MAX(product_service_rate) - MIN(product_service_rate)
            ELSE 0
        END AS price_volatility,
        
        -- Revenue-weighted average price
        CASE 
            WHEN SUM(COALESCE(product_service_quantity, 0)) > 0 
            THEN SUM(product_service_amount) / SUM(COALESCE(product_service_quantity, 0))
            ELSE AVG(product_service_rate)
        END AS volume_weighted_price,
        
        -- Source mix
        COUNT(CASE WHEN source_type = 'invoice' THEN 1 END) AS invoice_transactions,
        COUNT(CASE WHEN source_type = 'sales_receipt' THEN 1 END) AS sales_receipt_transactions,
        
        -- Metadata
        MAX(load_date) AS latest_load_date
        
    FROM transaction_data
    GROUP BY product_service, order_date
),

-- Get authoritative pricing from items table (when available)
authoritative_pricing AS (
    SELECT 
        item_name as product_service,
        CAST(sales_price AS NUMERIC) as authoritative_price,
        CASE 
            WHEN snapshot_date = 'seed' THEN '1900-01-01'::DATE
            WHEN snapshot_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN CAST(snapshot_date AS DATE)
            ELSE NULL
        END as price_effective_date
    FROM {{ ref('stg_quickbooks__items') }}
    WHERE sales_price IS NOT NULL 
        AND sales_price > 0
        AND snapshot_date IS NOT NULL
        AND (snapshot_date = 'seed' OR snapshot_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$')
),

-- Detect stable retail price periods using price clustering and frequency analysis
stable_price_periods AS (
    SELECT 
        product_service,
        order_date,
        avg_unit_price,
        total_units_sold,
        transaction_count,
        
        -- Identify if this is actually a product (not shipping, tax, etc.)
        CASE 
            WHEN LOWER(product_service) LIKE '%shipping%' THEN FALSE
            WHEN LOWER(product_service) LIKE '%tax%' THEN FALSE
            WHEN LOWER(product_service) LIKE '%freight%' THEN FALSE
            WHEN LOWER(product_service) LIKE '%discount%' THEN FALSE
            WHEN product_service ~ '^[0-9]' THEN TRUE  -- Product codes typically start with numbers
            WHEN LENGTH(product_service) >= 3 AND product_service != '' THEN TRUE
            ELSE FALSE
        END AS is_actual_product,
        
        -- Price clustering: group prices within $5 of each other (only for products)
        CASE 
            WHEN LOWER(product_service) NOT LIKE '%shipping%' 
                AND LOWER(product_service) NOT LIKE '%tax%' 
                AND LOWER(product_service) NOT LIKE '%freight%'
                AND LOWER(product_service) NOT LIKE '%discount%'
            THEN ROUND(avg_unit_price / 5) * 5 
            ELSE NULL
        END AS price_cluster,
        
        -- Calculate 30-day rolling statistics to identify stable periods (only for products)
        CASE 
            WHEN LOWER(product_service) NOT LIKE '%shipping%' 
                AND LOWER(product_service) NOT LIKE '%tax%' 
                AND LOWER(product_service) NOT LIKE '%freight%'
                AND LOWER(product_service) NOT LIKE '%discount%'
            THEN COUNT(*) OVER (
                PARTITION BY product_service, ROUND(avg_unit_price / 5) * 5
                ORDER BY order_date
                RANGE BETWEEN INTERVAL '30 days' PRECEDING AND CURRENT ROW
            )
            ELSE 0
        END AS cluster_frequency_30d,
        
        SUM(total_units_sold) OVER (
            PARTITION BY product_service, ROUND(avg_unit_price / 5) * 5
            ORDER BY order_date
            RANGE BETWEEN INTERVAL '30 days' PRECEDING AND CURRENT ROW
        ) AS cluster_volume_30d,
        
        -- Identify high-volume discount transactions (likely bulk sales)
        CASE 
            WHEN total_units_sold > 100 THEN 'HIGH_VOLUME_DISCOUNT'
            WHEN total_units_sold > 50 THEN 'MEDIUM_VOLUME'
            ELSE 'REGULAR_SALE'
        END AS volume_category
        
    FROM daily_pricing
),

-- Determine retail prices based on stability and frequency patterns
retail_price_detection AS (
    SELECT 
        *,
        
        -- Detect stable retail prices: frequent, multi-day patterns with regular volume (only for products)
        CASE 
            WHEN is_actual_product = TRUE
                AND cluster_frequency_30d >= 3 
                AND volume_category IN ('REGULAR_SALE', 'MEDIUM_VOLUME')
                AND price_cluster >= 65  -- Exclude obviously discounted prices
            THEN price_cluster
            ELSE NULL
        END AS stable_retail_candidate,
        
        -- Track the most common price in recent history (only for products)
        CASE 
            WHEN is_actual_product = TRUE AND price_cluster IS NOT NULL
            THEN FIRST_VALUE(price_cluster) OVER (
                PARTITION BY product_service, price_cluster
                ORDER BY cluster_frequency_30d DESC, order_date DESC
                ROWS UNBOUNDED PRECEDING
            )
            ELSE NULL
        END AS dominant_price_candidate
        
    FROM stable_price_periods
),

-- Calculate inferred retail price using stability analysis
pricing_with_retail_logic AS (
    SELECT
        *,
        
        -- Inferred retail price logic (only for actual products):
        -- 1. Use stable retail candidate if available
        -- 2. Fall back to dominant price over time
        -- 3. Use running maximum only if no stable pattern exists
        -- 4. Return NULL for non-products (shipping, tax, etc.)
        CASE 
            WHEN is_actual_product = FALSE THEN NULL
            ELSE COALESCE(
                stable_retail_candidate,
                CASE 
                    WHEN dominant_price_candidate >= 65 
                        AND cluster_frequency_30d >= 2
                    THEN dominant_price_candidate
                    ELSE NULL
                END,
                MAX(CASE WHEN volume_category = 'REGULAR_SALE' THEN avg_unit_price END) OVER (
                    PARTITION BY product_service 
                    ORDER BY order_date 
                    ROWS UNBOUNDED PRECEDING
                )
            )
        END AS inferred_retail_price
        
    FROM retail_price_detection
),

-- Add time-series analysis with retail price logic
pricing_with_trends AS (
    SELECT
        prl.*,
        
        -- Get authoritative price if available for this date
        ap.authoritative_price,
        ap.price_effective_date,
        
        -- Determine the retail price to use
        CASE 
            WHEN ap.authoritative_price IS NOT NULL THEN ap.authoritative_price
            ELSE prl.inferred_retail_price
        END AS retail_price_at_date,
        
        -- Price source indicator
        CASE 
            WHEN ap.authoritative_price IS NOT NULL THEN 'AUTHORITATIVE'
            WHEN prl.stable_retail_candidate IS NOT NULL THEN 'STABLE_PATTERN'
            WHEN prl.dominant_price_candidate IS NOT NULL 
                AND prl.cluster_frequency_30d >= 2 THEN 'DOMINANT_PRICE'
            ELSE 'FALLBACK_MAX'
        END AS retail_price_source,
        
        -- Previous price tracking
        LAG(prl.avg_unit_price) OVER (
            PARTITION BY prl.product_service 
            ORDER BY prl.order_date
        ) AS prev_avg_price,
        
        LAG(prl.order_date) OVER (
            PARTITION BY prl.product_service 
            ORDER BY prl.order_date
        ) AS prev_sale_date,
        
        -- Previous retail price
        LAG(
            CASE 
                WHEN ap.authoritative_price IS NOT NULL THEN ap.authoritative_price
                ELSE prl.inferred_retail_price
            END
        ) OVER (
            PARTITION BY prl.product_service 
            ORDER BY prl.order_date
        ) AS prev_retail_price,
        
        -- Enhanced price trend indicators
        CASE 
            WHEN LAG(prl.avg_unit_price) OVER (
                PARTITION BY prl.product_service 
                ORDER BY prl.order_date
            ) IS NULL THEN 'NEW'
            WHEN prl.avg_unit_price > LAG(prl.avg_unit_price) OVER (
                PARTITION BY prl.product_service 
                ORDER BY prl.order_date
            ) THEN 'INCREASING'
            WHEN prl.avg_unit_price < LAG(prl.avg_unit_price) OVER (
                PARTITION BY prl.product_service 
                ORDER BY prl.order_date
            ) THEN 'DECREASING'
            ELSE 'STABLE'
        END AS price_trend,
        
        -- Price change amount and percentage
        CASE 
            WHEN LAG(prl.avg_unit_price) OVER (
                PARTITION BY prl.product_service 
                ORDER BY prl.order_date
            ) IS NOT NULL 
            THEN prl.avg_unit_price - LAG(prl.avg_unit_price) OVER (
                PARTITION BY prl.product_service 
                ORDER BY prl.order_date
            )
            ELSE 0
        END AS price_change_amount,
        
        CASE 
            WHEN LAG(prl.avg_unit_price) OVER (
                PARTITION BY prl.product_service 
                ORDER BY prl.order_date
            ) IS NOT NULL 
            AND LAG(prl.avg_unit_price) OVER (
                PARTITION BY prl.product_service 
                ORDER BY prl.order_date
            ) > 0
            THEN ROUND(
                ((prl.avg_unit_price - LAG(prl.avg_unit_price) OVER (
                    PARTITION BY prl.product_service 
                    ORDER BY prl.order_date
                )) / LAG(prl.avg_unit_price) OVER (
                    PARTITION BY prl.product_service 
                    ORDER BY prl.order_date
                )) * 100, 2
            )
            ELSE 0
        END AS price_change_percentage
        
    FROM pricing_with_retail_logic prl
    LEFT JOIN authoritative_pricing ap
        ON prl.product_service = ap.product_service 
        AND prl.order_date >= ap.price_effective_date
        AND ap.price_effective_date = (
            SELECT MAX(price_effective_date) 
            FROM authoritative_pricing ap2 
            WHERE ap2.product_service = prl.product_service 
                AND ap2.price_effective_date <= prl.order_date
        )
),

-- Add product context and discount calculations
final_pricing_history AS (
    SELECT
        ph.*,
        
        -- Add days since last sale
        CASE 
            WHEN ph.prev_sale_date IS NOT NULL 
            THEN ph.order_date - ph.prev_sale_date
            ELSE 0
        END AS days_since_last_sale,
        
        -- Join with current product categorization
        p.product_family,
        p.material_type,
        p.is_kit,
        p.item_type,
        p.item_subtype,
        p.sales_price AS current_list_price,
        p.purchase_cost AS current_purchase_cost,
        
        -- Calculate discount from retail price at that date
        CASE 
            WHEN ph.retail_price_at_date IS NOT NULL AND ph.retail_price_at_date > 0
            THEN ROUND(((ph.avg_unit_price - ph.retail_price_at_date) / ph.retail_price_at_date) * 100, 2)
            ELSE NULL
        END AS discount_from_retail_pct,
        
        CASE 
            WHEN ph.retail_price_at_date IS NOT NULL 
            THEN ph.avg_unit_price - ph.retail_price_at_date
            ELSE NULL
        END AS discount_from_retail_amount,
        
        -- Retail price change detection
        CASE 
            WHEN ph.prev_retail_price IS NOT NULL AND ph.retail_price_at_date != ph.prev_retail_price
            THEN 'RETAIL_PRICE_CHANGE'
            WHEN ph.prev_retail_price IS NULL
            THEN 'NEW_PRODUCT'
            ELSE 'NO_RETAIL_CHANGE'
        END AS retail_price_change_type,
        
        -- Retail price change amount
        CASE 
            WHEN ph.prev_retail_price IS NOT NULL 
            THEN ph.retail_price_at_date - ph.prev_retail_price
            ELSE 0
        END AS retail_price_change_amount,
        
        -- Retail price change percentage
        CASE 
            WHEN ph.prev_retail_price IS NOT NULL AND ph.prev_retail_price > 0
            THEN ROUND(((ph.retail_price_at_date - ph.prev_retail_price) / ph.prev_retail_price) * 100, 2)
            ELSE 0
        END AS retail_price_change_pct,
        
        -- Legacy comparison to current list price (for backwards compatibility)
        CASE 
            WHEN p.sales_price IS NOT NULL AND p.sales_price > 0
            THEN ROUND(((ph.avg_unit_price - p.sales_price) / p.sales_price) * 100, 2)
            ELSE NULL
        END AS discount_from_current_list_pct,
        
        CASE 
            WHEN p.sales_price IS NOT NULL 
            THEN ph.avg_unit_price - p.sales_price
            ELSE NULL
        END AS discount_from_current_list_amount
        
    FROM pricing_with_trends ph
    LEFT JOIN {{ ref('fct_products') }} p
        ON ph.product_service = p.item_name
)

SELECT * FROM final_pricing_history
ORDER BY product_service, order_date DESC
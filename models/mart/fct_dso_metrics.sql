/*
ABOUTME: Days Sales Outstanding (DSO) key performance metrics and trends
ABOUTME: Calculates DSO, collection efficiency, and cash flow metrics over time
*/

{{ config(
    materialized = 'table',
    tags = ['finance', 'kpi', 'dso']
) }}

WITH current_ar AS (
    -- Current accounts receivable (open invoices)
    SELECT 
        SUM(total_amount) AS total_accounts_receivable,
        COUNT(*) AS open_invoice_count,
        AVG(total_amount) AS avg_open_invoice
    FROM {{ ref('fct_orders') }}
    WHERE sales_channel = 'Invoice' 
        AND status = 'OPEN'
),

recent_sales AS (
    -- Recent invoice sales for DSO calculation
    SELECT 
        period_days,
        SUM(total_amount) AS total_sales,
        COUNT(*) AS invoice_count,
        SUM(total_amount) / period_days AS daily_avg_sales
    FROM (
        SELECT 
            30 AS period_days,
            total_amount
        FROM {{ ref('fct_orders') }}
        WHERE sales_channel = 'Invoice' 
            AND order_date >= CURRENT_DATE - INTERVAL '30 days'
        
        UNION ALL
        
        SELECT 
            60 AS period_days,
            total_amount
        FROM {{ ref('fct_orders') }}
        WHERE sales_channel = 'Invoice' 
            AND order_date >= CURRENT_DATE - INTERVAL '60 days'
            
        UNION ALL
        
        SELECT 
            90 AS period_days,
            total_amount
        FROM {{ ref('fct_orders') }}
        WHERE sales_channel = 'Invoice' 
            AND order_date >= CURRENT_DATE - INTERVAL '90 days'
    ) recent_periods
    GROUP BY period_days
),

dso_calculations AS (
    SELECT 
        rs.period_days,
        rs.total_sales,
        rs.daily_avg_sales,
        ar.total_accounts_receivable,
        ar.open_invoice_count,
        -- Calculate DSO: AR / (Sales / Days)
        ROUND(ar.total_accounts_receivable / rs.daily_avg_sales, 1) AS dso_days,
        -- Collection efficiency (lower is better)
        ROUND((ar.total_accounts_receivable / rs.total_sales) * 100, 1) AS collection_efficiency_pct
    FROM recent_sales rs
    CROSS JOIN current_ar ar
),

segment_ar AS (
    -- AR breakdown by customer segment
    SELECT 
        customer_segment,
        COUNT(*) AS open_invoices,
        SUM(total_amount) AS segment_ar,
        AVG(total_amount) AS avg_invoice,
        ROUND(AVG(CURRENT_DATE - order_date), 1) AS avg_days_outstanding
    FROM {{ ref('fct_orders') }}
    WHERE sales_channel = 'Invoice' 
        AND status = 'OPEN'
    GROUP BY customer_segment
),

monthly_trends AS (
    -- Monthly DSO trends (last 6 months)
    SELECT 
        DATE_TRUNC('month', order_date) AS invoice_month,
        COUNT(*) AS invoices_created,
        SUM(total_amount) AS monthly_invoice_sales,
        -- Calculate month-end AR for trend analysis
        SUM(CASE WHEN status = 'OPEN' THEN total_amount ELSE 0 END) AS month_end_ar
    FROM {{ ref('fct_orders') }}
    WHERE sales_channel = 'Invoice'
        AND order_date >= CURRENT_DATE - INTERVAL '6 months'
    GROUP BY DATE_TRUNC('month', order_date)
)

-- Main metrics summary
SELECT 
    'Overall DSO Metrics' AS metric_category,
    period_days AS period,
    dso_days,
    collection_efficiency_pct,
    total_accounts_receivable,
    open_invoice_count,
    daily_avg_sales,
    -- DSO assessment
    CASE 
        WHEN dso_days <= 30 THEN 'Excellent'
        WHEN dso_days <= 35 THEN 'Good'  
        WHEN dso_days <= 45 THEN 'Fair'
        ELSE 'Poor'
    END AS dso_assessment,
    NULL AS customer_segment,
    NULL AS segment_ar,
    NULL AS avg_days_outstanding
FROM dso_calculations

UNION ALL

-- Segment breakdown
SELECT 
    'Segment AR Breakdown' AS metric_category,
    NULL AS period,
    NULL AS dso_days,
    NULL AS collection_efficiency_pct,
    segment_ar AS total_accounts_receivable,
    open_invoices AS open_invoice_count,
    NULL AS daily_avg_sales,
    CASE 
        WHEN avg_days_outstanding <= 30 THEN 'Good Payer'
        WHEN avg_days_outstanding <= 45 THEN 'Slow Payer'
        ELSE 'Problem Segment'
    END AS dso_assessment,
    customer_segment,
    segment_ar,
    avg_days_outstanding
FROM segment_ar

ORDER BY 
    metric_category,
    period ASC NULLS LAST,
    segment_ar DESC NULLS LAST
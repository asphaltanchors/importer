/*
ABOUTME: Days Sales Outstanding (DSO) and accounts receivable aging analysis
ABOUTME: Provides critical cash flow metrics and collection efficiency tracking
*/

{{ config(
    materialized = 'table',
    tags = ['finance', 'cash_flow', 'dso']
) }}

WITH open_invoices AS (
    SELECT 
        order_number,
        customer,
        customer_segment,
        order_date,
        due_date,
        total_amount,
        terms,
        -- Calculate days outstanding
        CURRENT_DATE - order_date AS days_outstanding,
        
        -- Calculate days past due (if due_date exists)
        CASE 
            WHEN due_date IS NOT NULL AND CURRENT_DATE > due_date 
            THEN CURRENT_DATE - due_date
            ELSE 0
        END AS days_past_due,
        
        -- Aging buckets
        CASE 
            WHEN CURRENT_DATE - order_date <= 30 THEN 'Current (0-30 days)'
            WHEN CURRENT_DATE - order_date <= 60 THEN 'Past Due (31-60 days)'
            WHEN CURRENT_DATE - order_date <= 90 THEN 'Overdue (61-90 days)'
            ELSE 'Severely Overdue (90+ days)'
        END AS aging_bucket,
        
        -- Risk assessment
        CASE 
            WHEN CURRENT_DATE - order_date <= 30 THEN 'Low Risk'
            WHEN CURRENT_DATE - order_date <= 60 THEN 'Medium Risk'
            WHEN CURRENT_DATE - order_date <= 90 THEN 'High Risk'
            ELSE 'Critical Risk'
        END AS collection_risk
        
    FROM {{ ref('fct_orders') }}
    WHERE sales_channel = 'Invoice'
        AND status = 'OPEN'
        AND order_date IS NOT NULL
),

aging_summary AS (
    SELECT 
        aging_bucket,
        collection_risk,
        COUNT(*) AS invoice_count,
        SUM(total_amount) AS total_amount,
        AVG(total_amount) AS avg_invoice_amount,
        AVG(days_outstanding) AS avg_days_outstanding,
        MIN(days_outstanding) AS min_days_outstanding,
        MAX(days_outstanding) AS max_days_outstanding
    FROM open_invoices
    GROUP BY aging_bucket, collection_risk
),

customer_ar_summary AS (
    SELECT 
        customer,
        customer_segment,
        COUNT(*) AS open_invoice_count,
        SUM(total_amount) AS total_ar_amount,
        AVG(days_outstanding) AS avg_days_outstanding,
        MAX(days_outstanding) AS max_days_outstanding,
        -- Customer payment pattern assessment
        CASE 
            WHEN AVG(days_outstanding) <= 35 THEN 'Good Payer'
            WHEN AVG(days_outstanding) <= 60 THEN 'Slow Payer'
            ELSE 'Problem Account'
        END AS payment_pattern
    FROM open_invoices
    GROUP BY customer, customer_segment
)

SELECT 
    'Individual Invoices' AS analysis_level,
    order_number,
    customer,
    customer_segment,
    order_date,
    due_date,
    total_amount,
    terms,
    days_outstanding,
    days_past_due,
    aging_bucket,
    collection_risk,
    NULL::TEXT AS payment_pattern,
    NULL::INTEGER AS open_invoice_count,
    NULL::NUMERIC AS total_ar_amount,
    NULL::NUMERIC AS avg_days_outstanding,
    NULL::NUMERIC AS max_days_outstanding
FROM open_invoices

UNION ALL

SELECT 
    'Customer Summary' AS analysis_level,
    NULL AS order_number,
    customer,
    customer_segment,
    NULL AS order_date,
    NULL AS due_date,
    total_ar_amount AS total_amount,
    NULL AS terms,
    NULL AS days_outstanding,
    NULL AS days_past_due,
    NULL AS aging_bucket,
    NULL AS collection_risk,
    payment_pattern,
    open_invoice_count,
    total_ar_amount,
    avg_days_outstanding,
    max_days_outstanding
FROM customer_ar_summary

UNION ALL

SELECT 
    'Aging Summary' AS analysis_level,
    NULL AS order_number,
    aging_bucket AS customer,
    NULL AS customer_segment,
    NULL AS order_date,
    NULL AS due_date,
    total_amount,
    NULL AS terms,
    NULL AS days_outstanding,
    NULL AS days_past_due,
    aging_bucket,
    collection_risk,
    NULL AS payment_pattern,
    invoice_count AS open_invoice_count,
    total_amount AS total_ar_amount,
    avg_days_outstanding,
    max_days_outstanding
FROM aging_summary

ORDER BY 
    analysis_level,
    days_outstanding DESC NULLS LAST,
    total_amount DESC NULLS LAST
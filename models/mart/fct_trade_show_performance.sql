/*
ABOUTME: Aggregated trade show performance metrics by show
ABOUTME: Provides ROI analysis and show effectiveness comparison
*/

{{ config(
    materialized = 'table',
    tags = ['trade_shows', 'mart']
) }}

WITH leads AS (
    SELECT * FROM {{ ref('fct_trade_show_leads') }}
),

show_performance AS (
    SELECT
        -- Show identifiers
        show_name,
        show_date,
        show_location,
        show_rep,

        -- Lead collection metrics
        COUNT(*) AS total_leads_collected,
        COUNT(DISTINCT email) AS unique_emails_collected,
        COUNT(DISTINCT CASE WHEN has_valid_email THEN email END) AS valid_emails,
        COUNT(DISTINCT company_domain_key) AS companies_matched,

        -- Lead categorization
        SUM(CASE WHEN company_match_status = 'matched_existing_customer' THEN 1 ELSE 0 END) AS leads_matched_to_companies,
        SUM(CASE WHEN company_match_status = 'unmatched' THEN 1 ELSE 0 END) AS leads_unmatched,
        SUM(CASE WHEN email_type = 'personal_email' THEN 1 ELSE 0 END) AS leads_individual_emails,

        -- Company-level attribution details
        SUM(CASE WHEN lead_email_is_customer THEN 1 ELSE 0 END) AS leads_who_are_direct_customers,
        SUM(CASE WHEN NOT lead_email_is_customer AND attributed_all_time THEN 1 ELSE 0 END) AS leads_attributed_via_company_colleagues,
        SUM(distinct_purchasers_count) AS total_distinct_purchasers,

        -- New customer acquisition
        SUM(CASE WHEN is_new_customer_from_show THEN 1 ELSE 0 END) AS new_customers_acquired,

        -- Attribution metrics (30 days)
        SUM(CASE WHEN attributed_30d THEN 1 ELSE 0 END) AS conversions_30d,
        SUM(revenue_30d) AS total_revenue_30d,

        -- Attribution metrics (90 days)
        SUM(CASE WHEN attributed_90d THEN 1 ELSE 0 END) AS conversions_90d,
        SUM(revenue_90d) AS total_revenue_90d,
        SUM(orders_90d) AS total_orders_90d,

        -- Attribution metrics (180 days)
        SUM(CASE WHEN attributed_180d THEN 1 ELSE 0 END) AS conversions_180d,
        SUM(revenue_180d) AS total_revenue_180d,

        -- Attribution metrics (365 days)
        SUM(CASE WHEN attributed_365d THEN 1 ELSE 0 END) AS conversions_365d,
        SUM(revenue_365d) AS total_revenue_365d,
        SUM(orders_365d) AS total_orders_365d,

        -- Attribution metrics (all time - no date limit)
        SUM(CASE WHEN attributed_all_time THEN 1 ELSE 0 END) AS conversions_all_time,
        SUM(revenue_all_time) AS total_revenue_all_time,
        SUM(orders_all_time) AS total_orders_all_time,

        -- Average metrics
        AVG(CASE WHEN attributed_90d THEN days_from_show_to_first_order END) AS avg_days_to_conversion_90d,
        AVG(CASE WHEN attributed_365d THEN days_from_show_to_first_order END) AS avg_days_to_conversion_365d,
        AVG(CASE WHEN attributed_all_time THEN days_from_show_to_first_order END) AS avg_days_to_conversion_all_time

    FROM leads
    GROUP BY show_name, show_date, show_location, show_rep
),

-- Calculate derived metrics
show_metrics AS (
    SELECT
        *,

        -- Match and conversion rates
        ROUND(
            CASE WHEN total_leads_collected > 0
            THEN (leads_matched_to_companies::NUMERIC / total_leads_collected::NUMERIC) * 100
            ELSE 0 END,
        2) AS match_rate_pct,

        ROUND(
            CASE WHEN leads_matched_to_companies > 0
            THEN (conversions_90d::NUMERIC / leads_matched_to_companies::NUMERIC) * 100
            ELSE 0 END,
        2) AS conversion_rate_90d_pct,

        ROUND(
            CASE WHEN leads_matched_to_companies > 0
            THEN (conversions_365d::NUMERIC / leads_matched_to_companies::NUMERIC) * 100
            ELSE 0 END,
        2) AS conversion_rate_365d_pct,

        ROUND(
            CASE WHEN leads_matched_to_companies > 0
            THEN (conversions_all_time::NUMERIC / leads_matched_to_companies::NUMERIC) * 100
            ELSE 0 END,
        2) AS conversion_rate_all_time_pct,

        -- Revenue per lead
        ROUND(
            CASE WHEN total_leads_collected > 0
            THEN total_revenue_90d::NUMERIC / total_leads_collected::NUMERIC
            ELSE 0 END,
        2) AS revenue_per_lead_90d,

        ROUND(
            CASE WHEN total_leads_collected > 0
            THEN total_revenue_365d::NUMERIC / total_leads_collected::NUMERIC
            ELSE 0 END,
        2) AS revenue_per_lead_365d,

        ROUND(
            CASE WHEN total_leads_collected > 0
            THEN total_revenue_all_time::NUMERIC / total_leads_collected::NUMERIC
            ELSE 0 END,
        2) AS revenue_per_lead_all_time,

        -- Show performance rating
        CASE
            WHEN conversions_90d >= 10 AND total_revenue_90d >= 50000 THEN 'Excellent'
            WHEN conversions_90d >= 5 AND total_revenue_90d >= 25000 THEN 'Good'
            WHEN conversions_90d >= 2 AND total_revenue_90d >= 10000 THEN 'Fair'
            ELSE 'Poor'
        END AS show_performance_90d,

        CASE
            WHEN conversions_365d >= 20 AND total_revenue_365d >= 100000 THEN 'Excellent'
            WHEN conversions_365d >= 10 AND total_revenue_365d >= 50000 THEN 'Good'
            WHEN conversions_365d >= 5 AND total_revenue_365d >= 25000 THEN 'Fair'
            ELSE 'Poor'
        END AS show_performance_365d,

        CASE
            WHEN conversions_all_time >= 30 AND total_revenue_all_time >= 150000 THEN 'Excellent'
            WHEN conversions_all_time >= 15 AND total_revenue_all_time >= 75000 THEN 'Good'
            WHEN conversions_all_time >= 8 AND total_revenue_all_time >= 40000 THEN 'Fair'
            ELSE 'Poor'
        END AS show_performance_all_time

    FROM show_performance
)

SELECT * FROM show_metrics

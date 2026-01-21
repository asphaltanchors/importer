/*
ABOUTME: Fact table for trade show leads with company matching and revenue attribution
ABOUTME: Includes attribution windows (30d, 90d, 180d, 365d) for measuring show effectiveness
*/

{{ config(
    materialized = 'table',
    tags = ['trade_shows', 'mart']
) }}

WITH leads_matched AS (
    SELECT * FROM {{ ref('int_trade_shows__leads_with_company_match') }}
),

-- Get orders for attribution matching
orders AS (
    SELECT
        order_date,
        customer,
        total_amount,
        order_number
    FROM {{ ref('fct_orders') }}
),

-- Get company orders for attribution (company-level)
company_orders AS (
    SELECT
        o.order_date,
        o.total_amount,
        o.order_number,
        bc.company_domain_key,
        bc.main_email as purchaser_email
    FROM orders o
    INNER JOIN {{ ref('bridge_customer_company') }} bc
        ON o.customer = bc.customer_name
),

-- Get customer emails for person-level matching
customer_emails AS (
    SELECT DISTINCT
        company_domain_key,
        LOWER(main_email) as customer_email
    FROM {{ ref('bridge_customer_company') }}
    WHERE main_email IS NOT NULL
),

-- Calculate attribution for each lead
leads_with_attribution AS (
    SELECT
        lm.*,

        -- Calculate days from show to first order (if company has orders)
        CASE
            WHEN lm.company_first_order_date IS NOT NULL AND lm.show_date IS NOT NULL
            THEN lm.company_first_order_date - lm.show_date
            ELSE NULL
        END AS days_from_show_to_first_order,

        -- Attribution windows
        CASE
            WHEN lm.company_first_order_date IS NOT NULL
                AND lm.show_date IS NOT NULL
                AND lm.company_first_order_date >= lm.show_date
                AND lm.company_first_order_date <= lm.show_date + INTERVAL '30 days'
            THEN TRUE
            ELSE FALSE
        END AS attributed_30d,

        CASE
            WHEN lm.company_first_order_date IS NOT NULL
                AND lm.show_date IS NOT NULL
                AND lm.company_first_order_date >= lm.show_date
                AND lm.company_first_order_date <= lm.show_date + INTERVAL '90 days'
            THEN TRUE
            ELSE FALSE
        END AS attributed_90d,

        CASE
            WHEN lm.company_first_order_date IS NOT NULL
                AND lm.show_date IS NOT NULL
                AND lm.company_first_order_date >= lm.show_date
                AND lm.company_first_order_date <= lm.show_date + INTERVAL '180 days'
            THEN TRUE
            ELSE FALSE
        END AS attributed_180d,

        CASE
            WHEN lm.company_first_order_date IS NOT NULL
                AND lm.show_date IS NOT NULL
                AND lm.company_first_order_date >= lm.show_date
                AND lm.company_first_order_date <= lm.show_date + INTERVAL '365 days'
            THEN TRUE
            ELSE FALSE
        END AS attributed_365d,

        -- Attribution: all time (no date limit)
        -- If company has ANY orders after the show date, attribute them
        CASE
            WHEN lm.company_first_order_date IS NOT NULL
                AND lm.show_date IS NOT NULL
                AND lm.company_first_order_date >= lm.show_date
            THEN TRUE
            ELSE FALSE
        END AS attributed_all_time,

        -- Calculate revenue in attribution windows
        (
            SELECT COALESCE(SUM(co.total_amount), 0)
            FROM company_orders co
            WHERE co.company_domain_key = lm.company_domain_key
                AND co.order_date >= lm.show_date
                AND co.order_date <= lm.show_date + INTERVAL '30 days'
        ) AS revenue_30d,

        (
            SELECT COALESCE(SUM(co.total_amount), 0)
            FROM company_orders co
            WHERE co.company_domain_key = lm.company_domain_key
                AND co.order_date >= lm.show_date
                AND co.order_date <= lm.show_date + INTERVAL '90 days'
        ) AS revenue_90d,

        (
            SELECT COALESCE(SUM(co.total_amount), 0)
            FROM company_orders co
            WHERE co.company_domain_key = lm.company_domain_key
                AND co.order_date >= lm.show_date
                AND co.order_date <= lm.show_date + INTERVAL '180 days'
        ) AS revenue_180d,

        (
            SELECT COALESCE(SUM(co.total_amount), 0)
            FROM company_orders co
            WHERE co.company_domain_key = lm.company_domain_key
                AND co.order_date >= lm.show_date
                AND co.order_date <= lm.show_date + INTERVAL '365 days'
        ) AS revenue_365d,

        -- Count orders in attribution windows
        (
            SELECT COALESCE(COUNT(*), 0)
            FROM company_orders co
            WHERE co.company_domain_key = lm.company_domain_key
                AND co.order_date >= lm.show_date
                AND co.order_date <= lm.show_date + INTERVAL '90 days'
        ) AS orders_90d,

        (
            SELECT COALESCE(COUNT(*), 0)
            FROM company_orders co
            WHERE co.company_domain_key = lm.company_domain_key
                AND co.order_date >= lm.show_date
                AND co.order_date <= lm.show_date + INTERVAL '365 days'
        ) AS orders_365d,

        -- All-time attribution (no date limit)
        (
            SELECT COALESCE(SUM(co.total_amount), 0)
            FROM company_orders co
            WHERE co.company_domain_key = lm.company_domain_key
                AND co.order_date >= lm.show_date
        ) AS revenue_all_time,

        (
            SELECT COALESCE(COUNT(*), 0)
            FROM company_orders co
            WHERE co.company_domain_key = lm.company_domain_key
                AND co.order_date >= lm.show_date
        ) AS orders_all_time

    FROM leads_matched lm
),

-- Add lead dimension key and company-level attribution details
final_leads AS (
    SELECT
        -- Lead identifiers
        lwa.lead_id,
        MD5(CONCAT(COALESCE(lwa.lead_id, ''), '|', COALESCE(lwa.show_name, ''))) AS lead_dim_key,

        -- Show information
        lwa.show_name,
        lwa.show_date,
        lwa.show_location,
        lwa.show_rep,

        -- Lead personal information
        lwa.full_name,
        lwa.first_name,
        lwa.last_name,
        lwa.email,
        lwa.phone,
        lwa.company AS lead_company_name,
        lwa.title,

        -- Address
        lwa.address_1,
        lwa.address_2,
        lwa.city,
        lwa.state,
        lwa.postal_code,
        lwa.country,

        -- Email classification
        lwa.email_domain,
        lwa.email_type,

        -- Company matching
        lwa.company_domain_key,
        lwa.consolidated_company_name,
        lwa.company_match_status,
        lwa.is_new_customer_from_show,

        -- Person-level vs company-level attribution
        -- Check if THIS specific lead's email is a customer
        CASE WHEN ce.customer_email IS NOT NULL THEN TRUE ELSE FALSE END AS lead_email_is_customer,

        -- Count distinct purchasers at this company (after show date)
        (
            SELECT COUNT(DISTINCT co.purchaser_email)
            FROM company_orders co
            WHERE co.company_domain_key = lwa.company_domain_key
                AND co.order_date >= lwa.show_date
        ) AS distinct_purchasers_count,

        -- Company metrics
        lwa.company_lifetime_revenue,
        lwa.company_lifetime_orders,
        lwa.company_first_order_date,
        lwa.company_latest_order_date,

        -- Attribution metrics (company-level: includes all purchases by anyone at the company)
        lwa.days_from_show_to_first_order,
        lwa.attributed_30d,
        lwa.attributed_90d,
        lwa.attributed_180d,
        lwa.attributed_365d,
        lwa.attributed_all_time,
        lwa.revenue_30d,
        lwa.revenue_90d,
        lwa.revenue_180d,
        lwa.revenue_365d,
        lwa.revenue_all_time,
        lwa.orders_90d,
        lwa.orders_365d,
        lwa.orders_all_time,

        -- Data quality
        lwa.has_valid_email,
        lwa.has_company_name,
        lwa.has_phone,

        -- Metadata
        lwa.source_id,
        lwa.notes,
        lwa.created_at,
        lwa.updated_at,
        lwa.load_date

    FROM leads_with_attribution lwa
    LEFT JOIN customer_emails ce
        ON lwa.company_domain_key = ce.company_domain_key
        AND LOWER(lwa.email) = ce.customer_email
)

SELECT * FROM final_leads

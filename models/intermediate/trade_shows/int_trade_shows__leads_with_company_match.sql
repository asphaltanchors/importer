/*
ABOUTME: Intermediate model matching trade show leads to existing companies
ABOUTME: Uses domain-based matching with existing company consolidation logic
*/

{{ config(
    materialized = 'table',
    tags = ['trade_shows', 'intermediate']
) }}

WITH leads AS (
    SELECT * FROM {{ ref('stg_trade_shows__leads') }}
),

-- Get domain mapping from company consolidation
domain_mapping AS (
    SELECT * FROM {{ source('raw_data', 'domain_mapping') }}
),

-- Get company master data
companies AS (
    SELECT * FROM {{ ref('fct_companies') }}
),

-- Match leads to companies via email domain
leads_matched AS (
    SELECT
        l.*,

        -- Company matching via domain
        dm.normalized_domain AS company_domain_key,
        dm.domain_type,

        -- Company information
        c.company_name AS consolidated_company_name,
        c.total_revenue AS company_lifetime_revenue,
        c.total_orders AS company_lifetime_orders,
        c.first_order_date AS company_first_order_date,
        c.latest_order_date AS company_latest_order_date,

        -- Match status
        CASE
            WHEN dm.normalized_domain IS NULL THEN 'unmatched'
            WHEN dm.domain_type = 'individual' THEN 'individual_email'
            WHEN dm.domain_type = 'skip' THEN 'marketplace'
            WHEN c.company_name IS NOT NULL THEN 'matched_existing_customer'
            ELSE 'matched_new_company'
        END AS company_match_status,

        -- New customer flag (company exists in our consolidation but no orders yet)
        CASE
            WHEN dm.normalized_domain IS NOT NULL
                AND dm.domain_type = 'corporate'
                AND (c.total_orders IS NULL OR c.total_orders = 0)
            THEN TRUE
            ELSE FALSE
        END AS is_new_customer_from_show

    FROM leads l
    LEFT JOIN domain_mapping dm
        ON l.email_domain = dm.original_domain
    LEFT JOIN companies c
        ON dm.normalized_domain = c.company_domain_key
)

SELECT * FROM leads_matched

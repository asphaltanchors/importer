/*
  This fact table model represents orders at the order level for business consumption.
  It builds on the intermediate model and adds additional business logic and metrics.
  
  Fact tables contain:
  - Business events/transactions
  - Foreign keys to dimensions
  - Business metrics/measures
*/

{{ config(
    materialized = 'table',
    tags = ['orders', 'quickbooks']
) }}

WITH orders AS (
    SELECT * FROM {{ ref('int_quickbooks__orders') }}
),

-- Get Shopify enrichment data for S- orders
shopify_enrichment AS (
    SELECT
        order_number_formatted as order_number,
        acquisition_channel,
        utm_source,
        utm_medium,
        utm_campaign,
        landing_site,
        referring_site,
        tracking_number,
        tracking_company,
        fulfilled_at,
        has_discount as shopify_has_discount
    FROM {{ ref('int_shopify__orders_enriched') }}
),

-- Get primary contacts for each customer to link orders to persons
customer_primary_contacts AS (
    SELECT 
        source_customer_name,
        contact_id,
        full_name as primary_contact_name,
        primary_email as primary_contact_email,
        primary_phone as primary_contact_phone,
        contact_role,
        company_domain_key
    FROM {{ ref('dim_customer_contacts') }}
    WHERE is_primary_company_contact = TRUE
),

-- Add any additional transformations or business logic here
orders_enriched AS (
    SELECT
        -- Primary key
        o.order_number,
        
        -- Order metadata
        o.source_type,
        o.order_date,
        o.customer,
        o.payment_method,
        o.status,
        o.due_date,
        
        -- Person/contact information
        cpc.contact_id as primary_contact_id,
        cpc.primary_contact_name,
        cpc.primary_contact_email,
        cpc.primary_contact_phone,
        cpc.contact_role as primary_contact_role,
        
        -- Flag fields
        o.is_tax_exempt,
        
        -- Additional flags/calculations
        CASE 
            WHEN o.status = 'PAID' THEN TRUE
            ELSE FALSE
        END AS is_paid,
        
        CASE
            WHEN o.due_date IS NOT NULL AND o.order_date IS NOT NULL AND o.due_date < o.order_date THEN TRUE
            ELSE FALSE
        END AS is_backdated,
        
        -- Addresses
        CONCAT_WS(', ',
            NULLIF(o.billing_address_line_1, ''),
            NULLIF(o.billing_address_line_2, ''),
            NULLIF(o.billing_address_line_3, '')
        ) AS billing_address,
        
        o.billing_address_city,
        o.billing_address_state,
        o.billing_address_postal_code,
        o.billing_address_country,
        
        CONCAT_WS(', ',
            NULLIF(o.shipping_address_line_1, ''),
            NULLIF(o.shipping_address_line_2, ''),
            NULLIF(o.shipping_address_line_3, '')
        ) AS shipping_address,
        
        o.shipping_address_city,
        o.shipping_address_state,
        o.shipping_address_postal_code,
        o.shipping_address_country,
        
        -- Country fields for reporting
        o.primary_country,
        o.country_category,
        o.region,
        
        -- Shipping information
        o.shipping_method,
        o.ship_date,

        -- Shopify enrichment (marketing attribution & fulfillment)
        CASE
            WHEN o.order_number LIKE 'S-%' THEN 'Shopify'
            WHEN o.order_number LIKE 'A%' THEN 'Direct'
            ELSE 'Other'
        END AS sales_channel_source,

        s.acquisition_channel,
        s.utm_source,
        s.utm_medium,
        s.utm_campaign,
        s.landing_site,
        s.referring_site,
        s.tracking_number,
        s.tracking_company,
        s.fulfilled_at,

        -- Order details
        o.memo,
        o.message_to_customer,
        o.class,
        o.currency,
        o.exchange_rate,
        o.terms,
        o.sales_rep,
        
        -- Channel and segment attribution
        o.sales_channel,
        o.customer_segment,
        
        -- Identifiers for joins
        o.transaction_id,
        o.quickbooks_internal_id,
        o.external_id,
        
        -- Dates for analytics
        o.created_date,
        o.modified_date,
        
        -- Metrics
        COALESCE(o.total_line_items_amount, 0.0) as total_line_items_amount,
        COALESCE(o.total_tax, 0.0) as total_tax,
        COALESCE(o.total_amount, 0.0) as total_amount,
        COALESCE(o.item_count, 0) as item_count,
        
        -- Derived metrics
        CASE 
            WHEN COALESCE(o.total_tax, 0) = 0 OR COALESCE(o.total_amount, 0) = 0 THEN 0
            ELSE ROUND(CAST((o.total_tax / o.total_amount) * 100 AS NUMERIC), 2)
        END AS effective_tax_rate
    FROM orders o
    LEFT JOIN customer_primary_contacts cpc ON o.customer = cpc.source_customer_name
    LEFT JOIN shopify_enrichment s ON o.order_number = s.order_number
)

SELECT * FROM orders_enriched
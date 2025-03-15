{{ config(materialized='table') }}

WITH company_data AS (
    SELECT
        quickbooks_id,
        company_domain,
        company_name
    FROM {{ ref('companies') }}
)

SELECT
    c."QuickBooks Internal Id" as quickbooks_id,
    c."Customer Name" as customer_name,
    c."First Name" as first_name,
    c."Last Name" as last_name,
    c."Customer Type" as customer_type,
    cd.company_domain,
    cd.company_name
FROM {{ source('raw', 'customers') }} c
LEFT JOIN company_data cd ON c."QuickBooks Internal Id" = cd.quickbooks_id

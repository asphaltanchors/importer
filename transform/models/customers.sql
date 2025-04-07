{{ config(materialized='table') }}

WITH customer_company_data AS (
  -- Extract domains from customer emails using the same logic as in companies.sql
  WITH email_domains AS (
    SELECT
      "QuickBooks Internal Id" as quickbooks_id,
      "Company Name" as company_name,
      "Main Email" as email_raw,
      -- Split the email string by semicolons
      regexp_split_to_table("Main Email", E';') as email_address
    FROM {{ source('raw', 'customers') }}
    WHERE "Main Email" IS NOT NULL
  ),

  extracted_domains AS (
    SELECT
      quickbooks_id,
      company_name,
      email_raw,
      -- Extract domain from each email address
      CASE
        WHEN POSITION('@' IN email_address) > 0 THEN 
          LOWER(TRIM(SUBSTRING(email_address FROM POSITION('@' IN email_address) + 1)))
        ELSE NULL
      END as domain
    FROM email_domains
    WHERE POSITION('@' IN email_address) > 0
  ),

  domain_counts AS (
    SELECT
      quickbooks_id,
      company_name,
      domain,
      COUNT(*) as domain_count
    FROM extracted_domains
    WHERE domain IS NOT NULL
    GROUP BY quickbooks_id, company_name, domain
  ),

  ranked_domains AS (
    SELECT
      quickbooks_id,
      company_name,
      domain,
      domain_count,
      ROW_NUMBER() OVER (PARTITION BY quickbooks_id ORDER BY domain_count DESC, domain) as domain_rank
    FROM domain_counts
  )

  SELECT
    c."QuickBooks Internal Id" as quickbooks_id,
    COALESCE(rd.domain, 
      CASE
        WHEN c."Main Email" IS NOT NULL AND POSITION('@' IN c."Main Email") > 0 THEN 
          SUBSTRING(c."Main Email" FROM POSITION('@' IN c."Main Email") + 1)
        ELSE NULL
      END
    ) as company_domain,
    c."Company Name" as company_name
  FROM {{ source('raw', 'customers') }} c
  LEFT JOIN ranked_domains rd ON c."QuickBooks Internal Id" = rd.quickbooks_id AND rd.domain_rank = 1
)

SELECT
    c."QuickBooks Internal Id" as quickbooks_id,
    c."Customer Name" as customer_name,
    c."First Name" as first_name,
    c."Last Name" as last_name,
    c."Customer Type" as customer_type,
    c."Billing Address City" as billing_city,
    c."Shipping Address City" as shipping_city,
    c."Main Email" as email,
    -- Only include the company_id as a foreign key
    comp.company_id
FROM {{ source('raw', 'customers') }} c
LEFT JOIN customer_company_data ccd ON c."QuickBooks Internal Id" = ccd.quickbooks_id
LEFT JOIN {{ ref('companies') }} comp ON 
    MD5(COALESCE(LOWER(TRIM(ccd.company_domain)), '')) = comp.company_id

{{ config(materialized='table') }}

-- This table contains one row per unique company
-- Companies are identified by their domain and name

WITH customer_domains AS (
  -- Extract domains from customer emails
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
  ),

  customer_company_data AS (
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
  ),
  
  -- Count occurrences of each company name per domain to find the most common
  company_name_counts AS (
    SELECT
      company_domain,
      company_name,
      COUNT(*) as name_count
    FROM customer_company_data
    WHERE company_domain IS NOT NULL
    GROUP BY company_domain, company_name
  ),
  
  -- Rank company names by frequency for each domain
  ranked_company_names AS (
    SELECT
      company_domain,
      company_name,
      name_count,
      ROW_NUMBER() OVER (PARTITION BY company_domain ORDER BY name_count DESC, company_name) as name_rank
    FROM company_name_counts
  )

  SELECT
    LOWER(TRIM(ccd.company_domain)) as normalized_domain,
    -- Use the most common company name for each domain
    MIN(rcn.company_name) as company_name,
    -- Keep one example of the original domain for display purposes
    MIN(ccd.company_domain) as company_domain,
    -- Use the first quickbooks_id as a reference (not used as PK)
    MIN(ccd.quickbooks_id) as reference_id
  FROM customer_company_data ccd
  LEFT JOIN ranked_company_names rcn ON 
    ccd.company_domain = rcn.company_domain AND rcn.name_rank = 1
  WHERE 
    -- Filter out records with empty domain
    COALESCE(LOWER(TRIM(ccd.company_domain)), '') != ''
  GROUP BY 
    -- Group by normalized domain only
    LOWER(TRIM(ccd.company_domain))
)

SELECT
  -- Create a surrogate key using MD5 hash of normalized domain only
  MD5(normalized_domain) as company_id,
  company_domain,
  company_name,
  reference_id,
  CURRENT_TIMESTAMP as created_at
FROM customer_domains

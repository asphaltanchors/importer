WITH email_domains AS (
    SELECT
        "QuickBooks Internal Id" AS quickbooks_id,
        "Company Name" AS company_name,
        "Main Email" AS email_raw,
        regexp_split_to_table("Main Email", E';') AS email_address
    FROM {{ source('raw', 'customers') }}
    WHERE "Main Email" IS NOT NULL
),
extracted_domains AS (
    SELECT
        quickbooks_id,
        company_name,
        email_raw,
        CASE
            WHEN POSITION('@' IN email_address) > 0 THEN 
                LOWER(TRIM(SUBSTRING(email_address FROM POSITION('@' IN email_address) + 1)))
            ELSE NULL
        END AS domain
    FROM email_domains
    WHERE POSITION('@' IN email_address) > 0
),
domain_counts AS (
    SELECT
        quickbooks_id,
        company_name,
        domain,
        COUNT(*) AS domain_count
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
        ROW_NUMBER() OVER (PARTITION BY quickbooks_id ORDER BY domain_count DESC, domain) AS domain_rank
    FROM domain_counts
),
customer_company_data AS (
    SELECT
        c."QuickBooks Internal Id" AS quickbooks_id,
        COALESCE(rd.domain, 
            CASE
                WHEN c."Main Email" IS NOT NULL AND POSITION('@' IN c."Main Email") > 0 THEN 
                    SUBSTRING(c."Main Email" FROM POSITION('@' IN c."Main Email") + 1)
                ELSE NULL
            END
        ) AS company_domain,
        c."Company Name" AS company_name
    FROM {{ source('raw', 'customers') }} c
    LEFT JOIN ranked_domains rd 
      ON c."QuickBooks Internal Id" = rd.quickbooks_id 
         AND rd.domain_rank = 1
),
ranked_company_names AS (
    SELECT
        company_domain,
        company_name,
        COUNT(*) AS name_count,
        ROW_NUMBER() OVER (PARTITION BY company_domain ORDER BY COUNT(*) DESC, company_name) AS name_rank
    FROM customer_company_data
    WHERE company_domain IS NOT NULL
    GROUP BY company_domain, company_name
),
domain_companies AS (
  -- ... (your CTE logic that produces 'most_common_company_name' and other fields)
  SELECT
      LOWER(TRIM(ccd.company_domain)) AS normalized_domain,
      MIN(rcn.company_name) AS most_common_company_name,
      MIN(ccd.company_domain) AS company_domain,
      MIN(ccd.quickbooks_id) AS reference_id
  FROM customer_company_data ccd
  LEFT JOIN ranked_company_names rcn 
    ON ccd.company_domain = rcn.company_domain 
       AND rcn.name_rank = 1
  WHERE COALESCE(LOWER(TRIM(ccd.company_domain)), '') != ''
  GROUP BY LOWER(TRIM(ccd.company_domain))
)

SELECT
    MD5(normalized_domain) AS company_id,
    company_domain,
    most_common_company_name,
    UPPER(most_common_company_name) AS company_name_upper,
    UPPER(
      TRIM(
        REGEXP_REPLACE(
          most_common_company_name,
          '\s*(,?\s*(LLC|L\.L\.C\.|INC|INC\.|INCORPORATED|CORPORATION|CORP|CORP\.|CO|CO\.|COMPANY|LTD|LTD\.|LIMITED|LP|L\.P\.|LLP|L\.L\.P\.|PC|P\.C\.|PLC|P\.L\.C\.|PLLC|P\.L\.L\.C\.))+\s*$',
          '',
          'gi'
        )
      )
    ) AS normalized_name,
    reference_id,
    CURRENT_TIMESTAMP AS created_at
FROM domain_companies
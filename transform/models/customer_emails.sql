{{ config(materialized='table') }}

WITH split_emails AS (
  SELECT
    "QuickBooks Internal Id" as quickbooks_id,
    "Customer Name" as customer_name,
    "Company Name" as company_name,
    -- Split the email string by semicolons and trim whitespace
    TRIM(regexp_split_to_table("Main Email", E';')) as email_address,
    ROW_NUMBER() OVER (
      PARTITION BY "QuickBooks Internal Id" 
      ORDER BY (TRIM(regexp_split_to_table("Main Email", E';')))
    ) as email_rank
  FROM {{ source('raw', 'customers') }}
  WHERE "Main Email" IS NOT NULL
)

SELECT
  quickbooks_id,
  customer_name,
  company_name,
  email_address,
  email_rank,
  -- Extract domain from email
  CASE
    WHEN POSITION('@' IN email_address) > 0 THEN 
      LOWER(SUBSTRING(email_address FROM POSITION('@' IN email_address) + 1))
    ELSE NULL
  END as email_domain,
  -- Is this the primary email (rank 1)?
  CASE WHEN email_rank = 1 THEN TRUE ELSE FALSE END as is_primary_email
FROM split_emails
WHERE email_address IS NOT NULL AND TRIM(email_address) != ''

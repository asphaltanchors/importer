{{ config(materialized='table') }}

SELECT
  "QuickBooks Internal Id" as quickbooks_id,
  CASE
    WHEN "Main Email" IS NOT NULL AND POSITION('@' IN "Main Email") > 0 THEN 
      SUBSTRING("Main Email" FROM POSITION('@' IN "Main Email") + 1)
    ELSE NULL
  END as company_domain,
  "Company Name" as company_name
FROM {{ source('raw', 'customers') }}

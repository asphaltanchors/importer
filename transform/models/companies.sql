-- Companies model that uses email domains as the primary identifier
WITH email_domains AS (
    SELECT
      "QuickBooks Internal Id" as quickbooks_id,
      "Company Name" as company_name,
      "Customer Name" as customer_name,
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
      customer_name,
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
      customer_name,
      domain,
      COUNT(*) as domain_count
    FROM extracted_domains
    WHERE domain IS NOT NULL
    GROUP BY quickbooks_id, company_name, customer_name, domain
),

ranked_domains AS (
    SELECT
      quickbooks_id,
      company_name,
      customer_name,
      domain,
      domain_count,
      ROW_NUMBER() OVER (PARTITION BY quickbooks_id ORDER BY domain_count DESC, domain) as domain_rank
    FROM domain_counts
),

customer_domains AS (
    SELECT
      c."QuickBooks Internal Id" as quickbooks_id,
      c."Company Name" as company_name,
      c."Customer Name" as customer_name,
      COALESCE(rd.domain, 
        CASE
          WHEN c."Main Email" IS NOT NULL AND POSITION('@' IN c."Main Email") > 0 THEN 
            LOWER(SUBSTRING(c."Main Email" FROM POSITION('@' IN c."Main Email") + 1))
          ELSE NULL
        END
      ) as company_domain,
      -- Convert Created Date to proper date format if it exists
      CASE
        WHEN c."Created Date" IS NOT NULL AND c."Created Date" != '' 
        THEN TO_DATE(c."Created Date", 'MM-DD-YYYY')
        ELSE NULL
      END as created_date
    FROM {{ source('raw', 'customers') }} c
    LEFT JOIN ranked_domains rd ON c."QuickBooks Internal Id" = rd.quickbooks_id AND rd.domain_rank = 1
    WHERE 
      -- Only include records with a valid domain
      (rd.domain IS NOT NULL OR 
       (c."Main Email" IS NOT NULL AND POSITION('@' IN c."Main Email") > 0))
),

unique_domains AS (
    SELECT DISTINCT
        company_domain,
        FIRST_VALUE(company_name) OVER (
            PARTITION BY company_domain 
            ORDER BY 
                -- Prefer non-null company names
                CASE WHEN company_name IS NOT NULL AND TRIM(company_name) != '' THEN 0 ELSE 1 END,
                -- Then use the first one alphabetically
                company_name
        ) AS company_name,
        FIRST_VALUE(customer_name) OVER (
            PARTITION BY company_domain 
            ORDER BY 
                -- Prefer non-null customer names
                CASE WHEN customer_name IS NOT NULL AND TRIM(customer_name) != '' THEN 0 ELSE 1 END,
                -- Then use the first one alphabetically
                customer_name
        ) AS customer_name,
        -- Get the earliest created date for this domain
        MIN(created_date) OVER (
            PARTITION BY company_domain
        ) AS earliest_created_date
    FROM customer_domains
    WHERE company_domain IS NOT NULL
),

-- Combine data from both sales_receipts and invoices
combined_transactions AS (
    -- Sales receipts data
    SELECT 
        c."Customer Name" as customer_name,
        -- Extract domain using the same method as email_domains CTE
        (SELECT domain FROM (
            SELECT LOWER(TRIM(SUBSTRING(email FROM POSITION('@' IN email) + 1))) as domain
            FROM regexp_split_to_table(c."Main Email", E';') as email
            WHERE POSITION('@' IN email) > 0
            LIMIT 1
        ) d) as company_domain,
        sr."Product/Service Class" as class,
        TO_DATE(sr."Sales Receipt Date", 'MM-DD-YYYY') as transaction_date,
        'sales_receipt' as source
    FROM {{ source('raw', 'sales_receipts') }} sr
    JOIN {{ source('raw', 'customers') }} c ON sr."Customer" = c."Customer Name"
    WHERE sr."Product/Service Class" IS NOT NULL 
      AND sr."Product/Service Class" != ''
      AND sr."Product/Service Class" != 'Unknown'
    
    UNION ALL
    
    -- Invoices data
    SELECT 
        c."Customer Name" as customer_name,
        -- Extract domain using the same method as email_domains CTE
        (SELECT domain FROM (
            SELECT LOWER(TRIM(SUBSTRING(email FROM POSITION('@' IN email) + 1))) as domain
            FROM regexp_split_to_table(c."Main Email", E';') as email
            WHERE POSITION('@' IN email) > 0
            LIMIT 1
        ) d) as company_domain,
        i."Product/Service Class" as class,
        TO_DATE(i."Invoice Date", 'MM-DD-YYYY') as transaction_date,
        'invoice' as source
    FROM {{ source('raw', 'invoices') }} i
    JOIN {{ source('raw', 'customers') }} c ON i."Customer" = c."Customer Name"
    WHERE i."Product/Service Class" IS NOT NULL 
      AND i."Product/Service Class" != ''
      AND i."Product/Service Class" != 'Unknown'
),

-- Rank classes by priority and recency
ranked_classes AS (
    SELECT
        company_domain,
        class,
        transaction_date,
        ROW_NUMBER() OVER (
            PARTITION BY company_domain 
            ORDER BY 
                -- If eStore is present with other classes, other classes take precedence
                CASE WHEN class = 'eStore' THEN 1 ELSE 0 END,
                -- Most recent date takes precedence for same priority classes
                transaction_date DESC
        ) as class_rank
    FROM combined_transactions
    WHERE company_domain IS NOT NULL
),

-- Get the highest priority class for each company
company_classes AS (
    SELECT DISTINCT
        company_domain,
        FIRST_VALUE(class) OVER (
            PARTITION BY company_domain
            ORDER BY class_rank
        ) as company_class
    FROM ranked_classes
),

consumer_domains AS (
    SELECT 
        UNNEST(ARRAY[
            'gmail.com',
            'yahoo.com',
            'yahoo.com.mx',
            'yahoo.com.br'
            'hotmail.com',
            'outlook.com',
            'aol.com',
            'icloud.com',
            'protonmail.com',
            'marketplace.amazon.com',
            'comcast.net',
            'verizon.net',
            'msn.com',
            'me.com',
            'att.net',
            'live.com',
            'bellsouth.net',
            'sbcglobal.net',
            'cox.net',
            'mac.com',
            'mail.com',
            'unknown-domain.com',
            'amazon-fba.com',
            'amazon.com',
            'zoho.com',
            'yandex.com',
            'gmx.com'
            -- Add more consumer domains as needed
        ]) AS domain
)

SELECT
    MD5(ud.company_domain) AS company_id,
    company_name,
    customer_name,
    ud.company_domain,
    -- Add the is_consumer_domain flag
    EXISTS (SELECT 1 FROM consumer_domains WHERE domain = ud.company_domain) AS is_consumer_domain,
    -- Add the class field
    -- If is_consumer_domain is true, always set class to 'consumer'
    -- Otherwise use the existing class logic
    CASE 
        WHEN EXISTS (SELECT 1 FROM consumer_domains WHERE domain = ud.company_domain) THEN 'consumer'
        ELSE COALESCE(cc.company_class, 'Unknown')
    END AS class,
    -- Use the earliest created date if available, otherwise use current date
    -- Cast to DATE type to remove time component
    COALESCE(earliest_created_date, CURRENT_DATE) AS created_at
FROM unique_domains ud
LEFT JOIN company_classes cc ON ud.company_domain = cc.company_domain

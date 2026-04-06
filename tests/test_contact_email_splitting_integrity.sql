/*
ABOUTME: Validates that email splitting doesn't lose or duplicate emails from the source data
ABOUTME: Ensures total split emails match the original semicolon-separated email counts
*/

-- Test that parsing retains the expected valid emails after applying the
-- same business rules as int_contact_email_parsing: valid syntax, no Amazon
-- marketplace emails, and case-insensitive deduplication per customer.
WITH raw_customers AS (
    SELECT
        quick_books_internal_id AS customer_id,
        main_email,
        cc_email
    FROM {{ source('raw_data', 'xlsx_customer') }}
),

main_emails_split AS (
    SELECT
        rc.customer_id,
        TRIM(email_part) AS individual_email,
        ROW_NUMBER() OVER (PARTITION BY rc.customer_id ORDER BY ord) AS email_position,
        'main' AS email_source
    FROM raw_customers rc
    CROSS JOIN UNNEST(STRING_TO_ARRAY(COALESCE(NULLIF(TRIM(rc.main_email), ''), ''), ';')) WITH ORDINALITY AS t(email_part, ord)
    WHERE TRIM(email_part) != '' AND email_part IS NOT NULL
),

cc_emails_split AS (
    SELECT
        rc.customer_id,
        TRIM(email_part) AS individual_email,
        ROW_NUMBER() OVER (PARTITION BY rc.customer_id ORDER BY ord) AS email_position,
        'cc' AS email_source
    FROM raw_customers rc
    CROSS JOIN UNNEST(STRING_TO_ARRAY(COALESCE(NULLIF(TRIM(rc.cc_email), ''), ''), ';')) WITH ORDINALITY AS t(email_part, ord)
    WHERE TRIM(email_part) != '' AND email_part IS NOT NULL
),

candidate_emails AS (
    SELECT * FROM main_emails_split
    UNION ALL
    SELECT * FROM cc_emails_split
),

expected_emails AS (
    SELECT
        customer_id,
        individual_email,
        email_source
    FROM (
        SELECT
            customer_id,
            individual_email,
            email_source,
            ROW_NUMBER() OVER (
                PARTITION BY customer_id, LOWER(individual_email)
                ORDER BY
                    CASE WHEN email_source = 'main' THEN 1 ELSE 2 END,
                    email_position
            ) AS email_rank
        FROM candidate_emails
        WHERE individual_email LIKE '%@%'
          AND SPLIT_PART(individual_email, '@', 1) != ''
          AND SPLIT_PART(individual_email, '@', 2) != ''
          AND LOWER(individual_email) NOT LIKE '%@marketplace.amazon.com'
    ) filtered_expected
    WHERE email_rank = 1
),

raw_email_counts AS (
    SELECT
        customer_id AS quick_books_internal_id,
        COUNT(CASE WHEN email_source = 'main' THEN 1 END) AS expected_main_emails,
        COUNT(CASE WHEN email_source = 'cc' THEN 1 END) AS expected_cc_emails
    FROM expected_emails
    GROUP BY customer_id
),

split_email_counts AS (
    SELECT 
        customer_id,
        COUNT(CASE WHEN email_source = 'main' THEN 1 END) as actual_main_emails,
        COUNT(CASE WHEN email_source = 'cc' THEN 1 END) as actual_cc_emails
    FROM {{ ref('int_contact_email_parsing') }}
    GROUP BY customer_id
),

mismatches AS (
    SELECT 
        r.quick_books_internal_id,
        r.expected_main_emails,
        COALESCE(s.actual_main_emails, 0) as actual_main_emails,
        r.expected_cc_emails,
        COALESCE(s.actual_cc_emails, 0) as actual_cc_emails
    FROM raw_email_counts r
    LEFT JOIN split_email_counts s ON r.quick_books_internal_id = s.customer_id
    WHERE r.expected_main_emails != COALESCE(s.actual_main_emails, 0)
       OR r.expected_cc_emails != COALESCE(s.actual_cc_emails, 0)
)

SELECT * FROM mismatches

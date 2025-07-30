/*
ABOUTME: Validates that email splitting doesn't lose or duplicate emails from the source data
ABOUTME: Ensures total split emails match the original semicolon-separated email counts
*/

-- Test that email splitting preserves all emails from source
-- Compare total split emails to expected count from raw data
WITH raw_email_counts AS (
    SELECT 
        quick_books_internal_id,
        -- Count semicolons + 1 for main emails (if not empty)
        CASE 
            WHEN TRIM(COALESCE(main_email, '')) = '' THEN 0
            ELSE ARRAY_LENGTH(STRING_TO_ARRAY(TRIM(main_email), ';'), 1)
        END as expected_main_emails,
        -- Count semicolons + 1 for cc emails (if not empty)  
        CASE 
            WHEN TRIM(COALESCE(cc_email, '')) = '' THEN 0
            ELSE ARRAY_LENGTH(STRING_TO_ARRAY(TRIM(cc_email), ';'), 1)
        END as expected_cc_emails
    FROM {{ source('raw_data', 'xlsx_customer') }}
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
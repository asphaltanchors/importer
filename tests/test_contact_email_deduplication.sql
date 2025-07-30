/*
ABOUTME: Critical test ensuring email deduplication logic works correctly across customer records
ABOUTME: Validates that the same email address doesn't appear multiple times in person mapping
*/

-- Test that email deduplication works correctly
-- No email should appear more than once in the person mapping table
SELECT 
    main_email,
    COUNT(*) as email_count
FROM {{ ref('int_quickbooks__customer_person_mapping') }}
WHERE main_email IS NOT NULL
GROUP BY main_email
HAVING COUNT(*) > 1
/*
ABOUTME: Ensures Amazon marketplace emails are properly filtered out from contact pipeline
ABOUTME: Validates business rule that anonymous marketplace emails should not be processed
*/

-- Test that Amazon marketplace emails are properly filtered
-- No Amazon marketplace emails should appear in any contact model
-- Test the new fixed models - return only rows where amazon emails were found
WITH test_results AS (
    SELECT 
        'staging_fixed' as model_layer,
        COUNT(*) as amazon_emails_found
    FROM {{ ref('int_contact_email_parsing') }}
    WHERE LOWER(individual_email) LIKE '%@marketplace.amazon.com'

    UNION ALL

    SELECT 
        'intermediate_fixed' as model_layer,
        COUNT(*) as amazon_emails_found  
    FROM {{ ref('int_customer_person_mapping_fixed') }}
    WHERE LOWER(main_email) LIKE '%@marketplace.amazon.com'
)

-- Only return rows where Amazon emails were found (should be empty)
SELECT * FROM test_results WHERE amazon_emails_found > 0
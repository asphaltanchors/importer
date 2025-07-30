/*
ABOUTME: Comprehensive data integrity test for the new contact pipeline architecture
ABOUTME: Validates that all key business rules and data quality constraints are met
*/

-- Test multiple aspects of the contact pipeline in one test
WITH pipeline_validation AS (
    -- Test 1: Email deduplication works (no duplicate emails in person mapping)
    SELECT 
        'email_deduplication' as test_name,
        COUNT(*) as failed_records
    FROM (
        SELECT main_email, COUNT(*) as email_count
        FROM {{ ref('int_customer_person_mapping_fixed') }}
        WHERE main_email IS NOT NULL
        GROUP BY main_email
        HAVING COUNT(*) > 1
    ) duplicates
    
    UNION ALL
    
    -- Test 2: No Amazon marketplace emails in pipeline
    SELECT 
        'amazon_filtering' as test_name,
        COUNT(*) as failed_records
    FROM {{ ref('int_customer_person_mapping_fixed') }}
    WHERE LOWER(main_email) LIKE '%@marketplace.amazon.com'
    
    UNION ALL
    
    -- Test 3: All contacts have valid company domain keys
    SELECT 
        'company_domain_mapping' as test_name,
        COUNT(*) as failed_records
    FROM {{ ref('int_customer_person_mapping_fixed') }}
    WHERE company_domain_key IS NULL OR company_domain_key = 'NO_EMAIL_DOMAIN'
    
    UNION ALL
    
    -- Test 4: Primary company contacts are properly flagged
    SELECT 
        'primary_contact_consistency' as test_name,
        COUNT(*) as failed_records
    FROM {{ ref('int_customer_person_mapping_fixed') }}
    WHERE (company_contact_rank = 1 AND is_primary_company_contact = FALSE)
       OR (company_contact_rank > 1 AND is_primary_company_contact = TRUE)
    
    UNION ALL
    
    -- Test 5: Contact quality scores are within valid range
    SELECT 
        'quality_score_range' as test_name,
        COUNT(*) as failed_records
    FROM {{ ref('int_customer_person_mapping_fixed') }}
    WHERE completeness_score < 0 OR completeness_score > 100
    
    UNION ALL
    
    -- Test 6: All contacts have person names
    SELECT 
        'person_name_required' as test_name,
        COUNT(*) as failed_records
    FROM {{ ref('int_customer_person_mapping_fixed') }}
    WHERE person_name IS NULL OR TRIM(person_name) = ''
)

-- Return any tests that failed (should be empty)
SELECT * FROM pipeline_validation WHERE failed_records > 0
/*
ABOUTME: Critical test for surrogate key stability - ensures keys don't change when email order changes
ABOUTME: This test will FAIL because current implementation includes email_position in hash
*/

-- Test that contact IDs remain stable when email positions shift
-- This test documents the current broken behavior
WITH contact_keys_with_position AS (
    SELECT 
        customer_id,
        main_email,
        contact_id,
        email_position,
        -- Generate what the key SHOULD be (without position)
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'main_email', 'email_source']) }} as stable_contact_id
    FROM {{ ref('stg_quickbooks__customer_contacts') }}
),

unstable_keys AS (
    SELECT 
        customer_id,
        main_email,
        COUNT(DISTINCT contact_id) as key_variations,
        COUNT(DISTINCT stable_contact_id) as should_be_variations
    FROM contact_keys_with_position
    GROUP BY customer_id, main_email
    HAVING COUNT(DISTINCT contact_id) != COUNT(DISTINCT stable_contact_id)
)

-- This will return rows showing the instability problem
SELECT 
    customer_id,
    main_email,
    key_variations,
    should_be_variations,
    'email_position in hash causes key instability' as issue
FROM unstable_keys
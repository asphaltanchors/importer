# Company Matching Findings and Implementation

## Overview

This document summarizes our findings and implementation for improving company name matching between orders and companies. The goal was to increase the match rate while maintaining reasonable query performance (under 60 seconds, ideally under 10 seconds).

## Initial State

The original implementation used simple case-insensitive exact matching:

```sql
-- Case-insensitive exact matches only
SELECT
  oc.order_id,
  c.company_id
FROM order_companies oc
JOIN companies_lower c ON oc.customer_name_lower = c.company_name_lower
```

**Initial Match Rate**:
- 2,209 orders matched (approximately 44.28% of orders)
- 50.67% of order amount matched

## Key Data Insights

Our analysis revealed several patterns in the data that affected matching:

1. **Comma-based name formats**: Many customer names use "Last, First" format while company names use "First Last" format
   - Example: "Market, Thrive" vs "Thrive Market"
   - Example: "Rivera, Carlos" vs "Carlos Rivera"

2. **Special notations**: Customer names often contain additional information that should be preserved
   - Discount percentages: "fastenal (30%):fastenal casac woodland ca"
   - Location information: "fastenal (30%):fastenal richmond bc canada"
   - Notes: "lausd (parent):lausd m&o n2 (see statement note)"

3. **Contact person information**: Many customer names include both company and contact person
   - Example: "United Hoisting & Scaffolding Corp, Covel"
   - Example: "Firstservice Residential, Ault, Eric"

4. **Typographical differences**: Minor spelling variations between otherwise identical names
   - Example: "waste management" vs "waste managment"
   - Example: "brandsafway" vs "brand safway"

## Implemented Solution

We implemented a two-part solution:

### 1. Normalized Company Names

We modified the `companies.sql` model to add normalized versions of company names:

```sql
-- Get domain companies with normalized names
domain_companies AS (
  SELECT
    normalized_domain,
    company_name,
    company_domain,
    reference_id,
    -- Simple uppercase version for basic case-insensitive matching
    UPPER(company_name) AS company_name_upper,
    -- Apply normalization logic to company names
    CASE WHEN 
      POSITION('%' IN company_name) > 0 OR
      POSITION(':' IN company_name) > 0 OR
      POSITION('(' IN company_name) > 0
    THEN
      -- Just normalize case and whitespace for special names
      UPPER(REGEXP_REPLACE(TRIM(company_name), '\\s+', ' '))
    ELSE
      -- For simple names, handle comma-based names
      CASE WHEN POSITION(',' IN company_name) > 0 THEN
        -- "Last, First" becomes "FIRST LAST"
        UPPER(
          TRIM(SPLIT_PART(company_name, ',', 2)) || ' ' || 
          TRIM(SPLIT_PART(company_name, ',', 1))
        )
      ELSE
        -- Just normalize case and whitespace
        UPPER(REGEXP_REPLACE(TRIM(company_name), '\\s+', ' '))
      END
    END AS normalized_name
  FROM ...
)
```

### 2. Two-Stage Matching

We implemented a two-stage matching process in `order_companies.sql`:

```sql
-- 1. First try original exact match (case-insensitive)
exact_matches AS (
  SELECT DISTINCT
    oc.order_id,
    c.company_id
  FROM order_companies oc
  JOIN companies c ON 
    oc.customer_name_upper = c.company_name_upper
),

-- 2. Then try normalized matches
normalized_matches AS (
  SELECT DISTINCT
    oc.order_id,
    c.company_id
  FROM order_companies oc
  JOIN companies c ON 
    oc.normalized_name = c.normalized_name
  WHERE
    oc.order_id NOT IN (SELECT order_id FROM exact_matches)
)

-- Combine both match types
SELECT * FROM exact_matches
UNION ALL
SELECT * FROM normalized_matches
```

## Results

Our implementation achieved:

1. **Improved Match Rate**:
   - Increased from 2,209 to 2,352 matches (+143 matches, ~6.5% improvement)
   - Most new matches were comma-based name formats (e.g., "Last, First" → "First Last")

2. **Excellent Performance**:
   - Query execution time: 0.11 seconds (down from several seconds in the original)
   - Well within our target of <10 seconds

## Additional Approaches Tested

We also tested more advanced matching techniques:

1. **Similarity Matching**:
   - Used PostgreSQL's `similarity` function with a threshold of 0.8
   - Found additional matches (2,577 total, +225 over normalized matching)
   - Performance impact: Increased execution time to 101.71 seconds

2. **Optimized Similarity Matching**:
   - Added length filtering and adaptive thresholds
   - Still too slow (105.19 seconds) with fewer matches (2,489)

## Key Learnings

1. **Normalization is effective**: Simple normalization techniques like handling comma-based names provide significant improvements with minimal performance impact.

2. **Similarity matching is powerful but expensive**: While similarity functions can find more matches, they come with a significant performance cost that may not be acceptable for production use.

3. **Preprocessing at storage time**: Normalizing company names when they're stored (in the companies table) rather than just at query time makes matching more efficient.

4. **Staged approach works best**: Applying cheaper matching techniques first and only using more expensive techniques for remaining unmatched records optimizes performance.

## Recommendations for Future Work

1. **Materialized intermediate tables**: For more advanced matching, consider materializing intermediate results to improve performance.

2. **Token-based matching**: Explore token-based approaches that match on significant words in company names.

3. **Machine learning**: For very large datasets, consider training a machine learning model on known matches to predict matches for new data.

4. **Regular data cleaning**: Implement processes to standardize company names at data entry time to reduce the need for complex matching.

5. **Feedback loop**: Create a system for users to confirm or reject suggested matches to improve matching over time.

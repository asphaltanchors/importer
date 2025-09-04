# Contact Pipeline Redesign Architecture

## Current Problems

1. **Staging Layer Violation**: `stg_quickbooks__customer_contacts.sql` contains 334 lines of complex business logic that belongs in intermediate models
2. **Unstable Surrogate Keys**: Contact IDs include `email_position` in hash, causing keys to change when email order shifts
3. **Broken Dimensional Joins**: Fact tables join to contacts using customer names instead of stable surrogate keys
4. **Missing Test Coverage**: Critical deduplication and email parsing logic lacks comprehensive testing

## Proposed Architecture

### Staging Layer (Basic Cleaning Only)
```
stg_quickbooks__customer_contacts.sql (REFACTORED - 50 lines max)
- Basic data type casting
- Column renaming and standardization  
- NULLIF cleaning
- NO business logic
```

### Intermediate Layer (Focused Business Logic)
```
int_contact_email_parsing.sql
- Email splitting from semicolon-separated fields
- Email source tracking (main vs cc)
- Email position tracking for context
- Basic email validation

int_contact_name_enrichment.sql  
- Name derivation from original fields
- Name extraction from email prefixes
- Full name construction logic
- Name title and job title handling

int_contact_quality_scoring.sql
- Contact data quality assessment
- Contact method priority assignment
- Completeness scoring
- Marketing/outreach flags

int_customer_person_mapping.sql (FIXED)
- Stable surrogate keys (NO email_position)
- Cross-customer email deduplication
- Company domain mapping integration
- Contact role assignment
```

### Mart Layer (Final Consumption)
```
dim_customer_contacts.sql (UPDATED)
- Final contact dimension with stable keys
- Company context enrichment
- Contact tier classifications
- Ready for fact table joins
```

## Key Design Principles

### 1. Stable Surrogate Keys
```sql
-- WRONG (current):
{{ dbt_utils.generate_surrogate_key(['customer_id', 'main_email', 'email_source', 'email_position']) }}

-- RIGHT (proposed):
{{ dbt_utils.generate_surrogate_key(['customer_id', 'main_email', 'email_source']) }}
```

### 2. Proper Dimensional Joins
```sql
-- WRONG (current): String-based joins
LEFT JOIN dim_customer_contacts dcc ON o.customer = dcc.source_customer_name

-- RIGHT (proposed): Stable key joins  
LEFT JOIN dim_customer_contacts dcc ON o.customer_contact_key = dcc.contact_dim_key
```

### 3. Layered Business Logic
- **Staging**: Only basic cleaning and standardization
- **Intermediate**: Single-purpose business transformations
- **Mart**: Final enrichment and presentation layer

### 4. Comprehensive Testing
- Email deduplication validation
- Surrogate key stability tests
- Data quality and completeness checks
- Amazon marketplace filtering verification

## Migration Strategy

### Phase 1: Create New Architecture (Parallel)
1. Build new intermediate models alongside existing ones
2. Create comprehensive test suite
3. Validate data integrity with current models

### Phase 2: Update Dependencies
1. Update mart models to use new intermediate models
2. Fix fact table joins to use stable keys
3. Update dashboard queries if needed

### Phase 3: Cleanup
1. Remove old staging model business logic
2. Archive obsolete models
3. Update documentation

## Testing Strategy

- **Automated Tests**: Schema tests in YAML files
- **Data Quality Tests**: Custom SQL tests for business rules
- **Integration Tests**: End-to-end pipeline validation
- **Performance Tests**: Validate query performance with new joins

## Benefits

1. **Maintainable**: Clear separation of concerns across layers
2. **Stable**: Surrogate keys won't break when data changes
3. **Testable**: Focused models enable targeted testing
4. **Performant**: Proper dimensional joins improve query performance
5. **Extensible**: Easy to add new contact sources or enrichment logic

## Timeline

- **Week 1**: Build new intermediate models and tests
- **Week 2**: Update mart models and fix joins  
- **Week 3**: Validate and deploy to production
- **Week 4**: Clean up old models and documentation
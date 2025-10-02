# XLSX Migration Open Issues

## Phase 1: Immediate DBT Compatibility (HIGH PRIORITY)

### Column Name Mapping Issues
**Problem**: XLSX standardization creates underscored column names that don't match DBT model expectations.

**Examples**:
- `product_service_amount` → `product_service__amount` 
- `quick_books_internal_id` → `QuickBooks_Internal_Id`
- Various spacing/special character standardizations

**Impact**: All staging models fail to compile
**Fix Required**: Update staging models to use correct XLSX column names

### Missing Column Handling
**Problem**: Some columns referenced in staging models don't exist in XLSX data.

**Examples**:
- `canonical_name` (referenced in customers staging)
- `class` (referenced in items staging) 
- `company_enrichment` table missing (no JSONL file in test data)

**Impact**: Models fail with "column does not exist" errors
**Fix Required**: Handle missing columns gracefully (COALESCE, conditional logic)

### Data Type Differences
**Problem**: XLSX provides better typing than CSV, but DBT models expect text for some fields.

**Examples**:
- `current_balance` now NUMERIC instead of text (breaks TRIM functions)
- Date fields may have different formats
- Percentage fields may be actual decimals vs strings

**Impact**: SQL compilation errors, runtime type mismatches
**Fix Required**: Update SQL to handle proper data types

## Phase 2: DBT Architecture Refactoring (MEDIUM PRIORITY)

### Anti-Pattern: Raw Table References in Mart Models
**Problem**: Mart models directly reference raw tables, violating DBT layering principles.

**Current Violations**:
- `fct_companies.sql` - Complex domain extraction logic in mart layer
- `bridge_customer_company.sql` - Direct raw table access, duplicated logic
- Revenue calculations in mart instead of intermediate

**Proper Architecture Should Be**:
```
Raw → Staging → Intermediate → Mart
```

**Fix Required**: 
1. Move business logic to intermediate models
2. Create proper intermediate models for reusable transformations
3. Update mart models to only reference intermediate/staging

### Missing Intermediate Models
**Problem**: Complex business logic is scattered across mart models instead of being centralized.

**Needed Intermediate Models**:
- `int_quickbooks__customer_domains` - Domain extraction and normalization
- `int_quickbooks__customer_revenue` - Customer revenue aggregations  
- `int_quickbooks__company_consolidation` - Company consolidation logic
- `int_quickbooks__order_enrichment` - Order-level business logic

**Benefits**: Reusability, testability, maintainability

## Phase 3: Pipeline Optimization (LOW PRIORITY)

### Column Standardization
**Problem**: XLSX column name standardization may be too aggressive.

**Current Logic**: 
```python
col.strip().replace('/', '_').replace(' ', '_').replace('.', '')
```

**Potential Issues**:
- Loses semantic meaning
- Creates very long column names
- May create conflicts

**Fix Required**: Review and refine standardization rules

### Data Type Inference Warnings
**Problem**: Many columns in XLSX worksheets have no data, causing DLT warnings.

**Impact**: Verbose logs, potential missing columns in destination
**Fix Required**: Add column type hints for empty columns

### Domain Consolidation Script Updates
**Problem**: Script still references old CSV table names and data types.

**Current Issue**: `TRIM(current_balance)` fails because field is now NUMERIC
**Fix Required**: Update script to handle XLSX data types

### Performance Considerations
**Problem**: Processing 23 worksheets vs 4 CSV files may impact performance.

**Considerations**:
- Memory usage with larger XLSX files
- Processing time for unused worksheets
- Database load with more tables

**Fix Required**: Monitor and optimize as needed

## Phase 4: Future Enhancements (BACKLOG)

### New Data Utilization
**Problem**: 19 new data sources available but not utilized.

**Opportunities**:
- Vendor management analytics
- Financial reporting (trial balance, journal entries)
- Employee data integration
- Advanced sales processes (estimates, sales orders)

**Fix Required**: Identify business value and implement gradually

### Datetime Deprecation
**Problem**: Pipeline uses deprecated `datetime.utcnow()`

**Fix Required**: Update to `datetime.now(datetime.UTC)`

### Error Handling Enhancement
**Problem**: Limited error handling for malformed XLSX files or missing worksheets.

**Fix Required**: Add robust error handling and data validation

---

## Implementation Priority

1. **IMMEDIATE**: Get DBT staging models working with XLSX data
2. **NEXT**: Refactor DBT architecture to proper layering
3. **THEN**: Address pipeline optimizations
4. **FUTURE**: Leverage new data sources for enhanced analytics

## Success Criteria

- [ ] All existing mart tables (`fct_orders`, `fct_products`, `fct_order_line_items`) work with XLSX data
- [ ] Dashboard queries continue working unchanged  
- [ ] DBT follows proper staging → intermediate → mart architecture
- [ ] Pipeline performance is acceptable
- [ ] All tests pass


--- Additional notes
- ensure historical information on items exists during switch over, open to backup/restore.

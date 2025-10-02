# XLSX Pipeline Architecture Design

## Overview
Clean migration from CSV to XLSX processing - replacing the existing CSV pipeline entirely.

## File Structure & Naming Convention

### XLSX File Types
- **Lists:** `all-list-*.xlsx` (accounts, vendors, employees, items, customers, etc.)
- **Transactions:** `all-transaction-*.xlsx` (invoices, sales receipts, payments, etc.)

### Naming Convention (Proposed)
```
# Full/Backup dumps
01_BACKUP_all-list_MM_DD_YYYY.xlsx
01_BACKUP_all-transaction_MM_DD_YYYY.xlsx

# Daily incrementals  
all-list_MM_DD_YYYY_H_MM_SS.xlsx
all-transaction_MM_DD_YYYY_H_MM_SS.xlsx
```

## DLT Resources Strategy

### Approach: One Resource Per Worksheet
Each of the 23 worksheets becomes a separate DLT resource:

**List Resources (6):**
- `xlsx_account` ← Account worksheet
- `xlsx_vendor` ← Vendor worksheet  
- `xlsx_employee` ← Employee worksheet
- `xlsx_item_discount` ← Item Discount worksheet
- `xlsx_item` ← Item worksheet
- `xlsx_customer` ← Customer worksheet

**Transaction Resources (17):**
- `xlsx_invoice` ← Invoice worksheet
- `xlsx_sales_order` ← Sales Order worksheet
- `xlsx_estimate` ← Estimate worksheet
- `xlsx_sales_receipt` ← Sales Receipt worksheet
- `xlsx_credit_memo` ← Credit Memo worksheet
- `xlsx_payment` ← Payment worksheet
- `xlsx_purchase_order` ← Purchase Order worksheet
- `xlsx_bill` ← Bill worksheet
- `xlsx_bill_payment` ← Bill Payment worksheet
- `xlsx_check` ← Check worksheet
- `xlsx_credit_card_charge` ← Credit Card Charge worksheet
- `xlsx_trial_balance` ← Trial Balance worksheet
- `xlsx_deposit` ← Deposit worksheet
- `xlsx_inventory_adjustment` ← Inventory Adjustment worksheet
- `xlsx_journal_entry` ← Journal Entry worksheet
- `xlsx_build_assembly` ← Build Assembly worksheet
- `xlsx_custom_txn_detail` ← Custom Txn Detail worksheet

## Entity Mapping for DBT Compatibility

### Core Entity Mapping
To maintain DBT compatibility, we'll create aliases in sources.yml:

```yaml
# Current DBT expects these table names:
customers → xlsx_customer
items → xlsx_item  
invoices → xlsx_invoice
sales_receipts → xlsx_sales_receipt
```

### Primary Key Strategy
- **All entities:** Use `QuickBooks Internal Id` as primary key
- **Line items:** Composite keys with `S.No` for ordering
- **Transactions:** Use `Trans #` + line item identifiers where needed

## Pipeline.py Architecture

```python
@dlt.source
def xlsx_quickbooks_source():
    # Lists processing
    for file_info in get_list_files():
        workbook = load_workbook(file_info['path'])
        for worksheet_name in LIST_WORKSHEETS:
            yield extract_worksheet(workbook, worksheet_name, file_info)
    
    # Transactions processing  
    for file_info in get_transaction_files():
        workbook = load_workbook(file_info['path'])
        for worksheet_name in TRANSACTION_WORKSHEETS:
            yield extract_worksheet(workbook, worksheet_name, file_info)
```

## Data Processing Logic

### File Precedence (Same as Current CSV)
1. **Backup files take precedence** over daily files for same date
2. **Latest daily file** used if multiple exist for same date
3. **Date extraction** from filename for snapshot_date

### Data Enrichment
- Add `load_date` (current timestamp)
- Add `snapshot_date` (extracted from filename)  
- Add `is_backup` (boolean flag)
- Add `source_file` (filename for traceability)

### Column Standardization
- **Date fields:** Parse MM-DD-YYYY strings to proper DATE type
- **Numeric fields:** Handle percentage strings, currency formatting
- **Text fields:** Trim whitespace, handle NULL/empty standardization
- **Boolean fields:** Standardize True/False/NULL values

## Raw Schema Structure

Raw tables will be created as:
```sql
raw.xlsx_customer     -- Customer worksheet data
raw.xlsx_item         -- Item worksheet data  
raw.xlsx_invoice      -- Invoice worksheet data
raw.xlsx_sales_receipt -- Sales Receipt worksheet data
-- ... (19 more tables for other worksheets)
```

## DBT Integration

### Sources Update
```yaml
# models/sources.yml
sources:
  - name: raw
    tables:
      # Legacy names maintained for compatibility
      - name: customers
        identifier: xlsx_customer
      - name: items  
        identifier: xlsx_item
      - name: invoices
        identifier: xlsx_invoice
      - name: sales_receipts
        identifier: xlsx_sales_receipt
      
      # New entities available
      - name: xlsx_vendor
      - name: xlsx_account
      # ... etc
```

### Staging Model Updates
- **No changes needed** to existing staging models
- **Column selection** may need updates for new fields
- **New staging models** can be added for new entities

## Migration Benefits

### Immediate
- **Richer data** in existing entities (61 customer fields vs 15)
- **Better data quality** (proper dates, standardized formats)
- **Single file processing** (vs multiple CSV files)

### Future Expansion Ready
- **23 entities available** for new analytics
- **Vendor management** capabilities
- **Financial reporting** data (accounts, trial balance)
- **Advanced sales processes** (estimates, sales orders)

## Implementation Steps

1. **Create new pipeline.py** with XLSX processing
2. **Update environment config** for XLSX file paths
3. **Modify sources.yml** with table aliases
4. **Test staging/mart compatibility** 
5. **Validate dashboard queries**
6. **Performance test** with full files

This design maintains backward compatibility while opening up significant expansion possibilities.
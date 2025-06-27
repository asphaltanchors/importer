# XLSX Analysis Report

## Overview

Analysis of QuickBooks XLSX export files to inform migration from CSV-based pipeline.

**Files Analyzed:**
- `all-list-test.xlsx` (70KB, 6 worksheets)  
- `all-transaction-test.xlsx` (851KB, 17 worksheets)

## Key Findings

### 1. Significantly Expanded Data Coverage

**Current CSV Pipeline covers 4 entities:**
- Customers
- Items 
- Sales Receipts
- Invoices

**XLSX Files cover 23+ entities:**
- **Lists (6):** Account, Vendor, Employee, Item Discount, Item, Customer
- **Transactions (17):** Invoice, Sales Order, Estimate, Sales Receipt, Credit Memo, Payment, Purchase Order, Bill, Bill Payment, Check, Credit Card Charge, Trial Balance, Deposit, Inventory Adjustment, Journal Entry, Build Assembly, Custom Txn Detail

### 2. Enhanced Data Fields

#### Customer Data Enrichment
**Current CSV:** Basic customer info (~15 fields)
**XLSX Customer:** 61 fields including:
- Complete address (billing + shipping with 5 lines each)
- Multiple contact methods (Main/Alt/Work phone, email, CC email, mobile, fax)
- Job management (Job Description, Type, Status, Start/Projected End/End dates)
- Enhanced business context (Industry, Source Channel, Price Level)
- Detailed notes and additional notes

#### Item/Product Data Enrichment  
**Current CSV:** Basic item info (~10-15 fields)
**XLSX Item:** 29 fields including:
- Detailed inventory tracking (Min/Max reorder points, quantities on hand/order/sales order)
- Enhanced product details (Unit weight, UPC, Manufacturer's Part Number)
- Complete accounting integration (COGS, Income, Expense, Asset accounts)
- Vendor relationships and purchasing info

#### Invoice/Transaction Data Enrichment
**Current CSV:** Basic transaction fields
**XLSX Invoice:** 73 fields including:
- Complete address information (billing + shipping with 5 lines each)
- Enhanced product/service details (inventory site, bin, serial numbers, lot numbers)
- Comprehensive shipping (shipping method, ship date, FOB terms)
- Advanced financial tracking (tax percentage, exchange rates, linked payments)
- Template and workflow management (print later, email later, external ID)

### 3. New Business Entities Not in CSV

#### Financial Management
- **Account:** Chart of accounts with 57 records (16 fields)
- **Trial Balance:** Financial reporting data
- **Journal Entry:** Manual accounting adjustments

#### Vendor/Supplier Management  
- **Vendor:** 13 vendors with complete contact/payment info (48 fields)
- **Purchase Order:** Purchasing workflow
- **Bill:** Vendor invoicing
- **Bill Payment:** AP management

#### Employee Management
- **Employee:** 1 employee record with HR data (45 fields)
- **Payroll integration** (referenced in accounts)

#### Advanced Sales Management
- **Sales Order:** Pre-invoice sales tracking
- **Estimate:** Quote management  
- **Credit Memo:** Returns/adjustments
- **Payment:** Cash receipt tracking

#### Inventory Management
- **Inventory Adjustment:** Stock level corrections
- **Build Assembly:** Manufacturing/kit assembly

### 4. Data Quality Observations

#### Consistent Primary Keys
- All entities have `QuickBooks Internal Id` field
- Sequential `S.No` for ordering
- Business keys available (Invoice No, Customer Name, Item Name, etc.)

#### Date Handling
- Consistent date formats (MM-DD-YYYY in strings)
- Created Date and Modified Date on all entities
- Transaction-specific dates (Invoice Date, Due Date, Ship Date, etc.)

#### Data Volume
- **Customers:** 155 records (vs current CSV ~dozens)
- **Items:** 49 records  
- **Invoices:** 1,000+ records (vs current CSV hundreds)
- **Additional entities:** 13 vendors, 57 accounts, various transaction types

## Migration Strategy Recommendations

### Phase 1: Core Entity Enhancement
1. **Extend existing resources** (customers, items, invoices, sales_receipts) with new XLSX fields
2. **Maintain backward compatibility** with current CSV processing
3. **Add new worksheet processing** for enhanced data

### Phase 2: New Entity Integration
1. **Financial entities:** Account, Trial Balance
2. **Vendor management:** Vendor, Purchase Order, Bill, Bill Payment  
3. **Advanced sales:** Sales Order, Estimate, Credit Memo, Payment

### Phase 3: Specialized Features
1. **Employee/HR data:** Employee records
2. **Inventory management:** Inventory Adjustment, Build Assembly
3. **Reporting:** Custom Transaction Detail

### Technical Implementation Notes

#### File Processing Strategy
- **Both files are manageable size** (<1MB each)
- **Full refresh approach recommended** due to comprehensive nature
- **Daily incremental processing** still viable with timestamp-based detection

#### Schema Mapping
- **1-to-1 worksheet mapping** to DLT resources
- **Composite primary keys** needed for line-item transactions
- **Date parsing standardization** required (MM-DD-YYYY strings to DATE)
- **Null handling** for extensive optional fields

#### DBT Impact
- **Staging models:** New models for each worksheet
- **Intermediate models:** Enhanced joins with new entity relationships  
- **Mart models:** Enriched fact tables with additional dimensions
- **Backward compatibility:** Ensure existing dashboard queries continue working

## Next Steps

1. **Design XLSX pipeline architecture** - dual CSV/XLSX support
2. **Create worksheet-specific DLT resources** for priority entities
3. **Update DBT models** to leverage enhanced data
4. **Plan migration timeline** with dashboard team coordination
5. **Performance testing** with full-size XLSX files
# Company Consolidation Implementation Plan

## Problem Analysis
After examining the data, the core issues are:
- Company names are polluted with pricing info: "Fasteners Plus (40%) 35%grout/epoxy"
- Same companies appear under multiple variations (Fastenal has 125+ customer records)
- Email domains are much cleaner identifiers than company names
- Top revenue customers (ZKxKZ: $302K, Fasteners Plus: $296K) have messy names

## Key Findings from Data Analysis
- **Fastenal**: 125 customer records, $2.7K total balance, multiple subdomains
- **Amazon marketplace**: 853 records but no visibility into individual sellers
- **High-value targets**: ZKxKZ ($302K), Fasteners Plus ($296K), Norfast ($279K)
- **Subdomain patterns**: Many companies use subdomains (stores.fastenal.com, etc.)

## Strategic Approach: Staged Implementation

### Phase 1: Domain Extraction & Normalization (Python) ✅
**Goal**: Create clean email domain → company mapping
- Extract primary email domains from customers table 
- Create domain normalization rules:
  - `*.fastenal.com` → `fastenal.com` (subdomain consolidation)
  - `marketplace.amazon.com` → skip/flag (no visibility into individual sellers)
  - Generic domains (gmail, yahoo) → keep separate/flag as individual
- Revenue-weight the consolidation (prioritize high-value customers)
- **Stop here for confirmation**

### Phase 2: Company Master Table (DBT)
**Goal**: Create `fct_companies` table with clean company profiles
- One row per consolidated company
- Primary key: `domain_key` (normalized email domain)
- Include: revenue totals, customer count, primary company name, address
- Flag corporate vs. individual customers
- **Stop here for confirmation**

### Phase 3: Customer-Company Bridge (DBT) 
**Goal**: Link existing customers to consolidated companies
- Map QuickBooks customers → company domain keys
- Preserve ability to drill down to individual customer level
- Handle edge cases (customers with no email, multiple domains)
- **Stop here for confirmation**

### Phase 4: Revenue Analytics (DBT)
**Goal**: Create company-level revenue views
- `fct_company_orders` - orders rolled up to company level
- Maintain line item detail for analysis
- Enable "who buys what" analysis by consolidated company
- **Stop here for confirmation**

## Implementation Notes
- Stop after each phase for user confirmation
- Skip Amazon marketplace processing (no visibility)
- Focus on 80/20 approach: get majority of revenue consolidated
- Use existing DLT/DBT infrastructure
- Maintain ability to drill down to individual customer level

## Key Files to Create/Modify
- `COMPANY_CONSOLIDATION_PLAN.md` (this plan document) ✅
- `domain_consolidation.py` (new Python script for domain rules)
- `models/mart/fct_companies.sql` (new company master table)
- `models/mart/fct_company_orders.sql` (new company-level revenue view)

## Phase 1 Results ✅
**Domain Analysis Complete**
- **Total customers with emails**: 5,487
- **Unique original domains**: 2,702  
- **Consolidated domains**: 2,695
- **High-impact consolidations**: Fastenal (147 customers), Amazon marketplace (853 - skipped)

**Key Findings**:
- **Fastenal**: Successfully consolidated 147 customers from multiple subdomains → $2.7K balance
- **Top revenue domains**: clickstop.com ($45K), usfloodcontrol.com ($16K), zkxkz.com ($15K)
- **Amazon marketplace**: 853 customers (correctly skipped - no visibility)
- **Individual emails**: 740 Gmail, 212 Yahoo (properly flagged as individuals)

**Normalization Rules Working**:
- Fastenal subdomains consolidated: stores.fastenal.com → fastenal.com
- Generic domains flagged: gmail.com → INDIVIDUAL_GMAIL.COM
- Amazon marketplace skipped correctly

## Phase 2 Results ✅
**Company Master Table Complete**
- **Total companies created**: 2,678 consolidated companies
- **Revenue properly mapped**: Top customers like ZKxKZ ($302K), Fasteners Plus ($296K), Norfast ($280K) 
- **Fastenal consolidation success**: 147 customer records → 1 company with $86K total revenue
- **Business classifications working**: Individual Customer, Single/Multi-Location categories
- **Revenue classifications working**: High Value ($100K+), Medium Value, etc.
- **All tests passing**: 12 data quality tests

**Key Achievements**:
- ✅ **Revenue linkage fixed**: Proper domain-based joins between customers and orders
- ✅ **Consolidation working**: Fastenal shows as "Large Multi-Location" (147 customers)  
- ✅ **Clean company names**: Best representative names chosen automatically
- ✅ **Business intelligence ready**: Revenue categories, size classifications
- ✅ **Data quality**: All uniqueness, completeness tests passing

## Phase 3 Results ✅
**Customer-Company Bridge Complete**
- **Total customer records**: 7,509 customers linked to consolidated companies
- **Drill-down capability**: Can see individual customers within each company
- **Customer classifications**: Value tiers, activity status, individual vs. corporate flags
- **Example success**: Fastenal shows 147 customers with top performer "Fastenal ILBEV" ($19K)
- **All tests passing**: 13 data quality tests including uniqueness and referential integrity

**Key Features**:
- ✅ **Perfect drill-down**: Company-level → Customer-level analysis enabled
- ✅ **Customer insights**: Value tiers, activity status, ordering patterns
- ✅ **Revenue attribution**: Individual customer revenue properly calculated
- ✅ **Analysis flags**: Individual customers, email status, revenue flags
- ✅ **Example queries**: Ready-to-use drill-down query patterns provided

**Bridge Table Capabilities**:
- Link from `fct_companies.company_domain_key` to `bridge_customer_company.company_domain_key`
- Customer-level metrics: revenue, orders, activity status, value classification
- Preserve all QuickBooks customer details for operational needs
- Enable analysis like "Who are Fastenal's top 10 customers?" or "Which customers are active?"

## Current Status
- **Phase 1**: ✅ COMPLETE - Domain extraction and normalization working well
- **Phase 2**: ✅ COMPLETE - Company Master Table created with proper revenue linkage  
- **Phase 3**: ✅ COMPLETE - Customer-Company Bridge enables drill-down analysis
- **Next**: Proceed to Phase 4: Company-level Revenue Analytics
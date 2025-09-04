# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a QuickBooks data pipeline combining DLT (Data Loading Tool) for extraction and DBT (Data Build Tool) for transformation. The pipeline processes QuickBooks XLSX exports (customers, items, sales receipts, invoices) from a Dropbox sync folder and loads them into PostgreSQL with transformations.

**Architecture**: Supports seed (historical) and incremental (daily) loading modes with proper directory structure separation.

**Dashboard Integration**: This pipeline feeds data to a NextJS dashboard application. The `fct_*` tables in the mart schema are the final output tables directly consumed by the dashboard for analytics and reporting.

**Cross-Project Communication**: The `DBT_CANDIDATES.md` file serves as an "API" between this data pipeline project and the dashboard application. It tracks:
- Data quality issues discovered during dashboard development
- Opportunities to move business logic from the dashboard into the DBT pipeline
- Schema improvements and pipeline enhancements needed for better dashboard performance
- Pipeline optimization candidates based on dashboard usage patterns

This file should be consulted when making pipeline improvements but should not be automatically implemented - it represents a backlog of potential improvements discovered through dashboard development.

## Key Commands

### Running the Pipeline
```bash
# Install dependencies
uv pip install -r requirements.txt

# Load all historical data from seed/ directory
python orchestrator.py --seed

# Load all available daily files from input/ directory  
python orchestrator.py --incremental

# Run pipeline for specific source only (multi-source support)
python orchestrator.py --source quickbooks --seed
python orchestrator.py --source quickbooks --incremental
```

### DBT Commands
**Note**: All DBT commands must be run from within the virtual environment.

```bash
# Activate virtual environment first
source .venv/bin/activate

# Run all DBT models
dbt run

# Run specific models
dbt run --select staging
dbt run --select intermediate
dbt run --select mart

# Test models
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

### Environment Setup
Create a `.env` file with:
```
DROPBOX_PATH=/path/to/dropbox/folder  # Parent directory containing seed/ and input/
DATABASE_URL=postgresql://user:password@host:port/dbname
```

### Directory Structure
```
/dropbox/quickbooks-csv/
├── seed/
│   ├── all_lists.xlsx           # Master customer/item data (one-time)
│   ├── all_transactions.xlsx    # Historical transactions (one-time)
│   └── company_enrichment.jsonl # External enrichment data
└── input/
    ├── {DATE}_transactions.xlsx  # Daily transaction increments
    └── {DATE}_lists.xlsx        # Daily list updates
```

## Architecture

### Data Flow
1. **Orchestrator** (`orchestrator.py`): Master pipeline coordinator that runs the complete data pipeline
   - Supports `--seed` (historical) and `--incremental` (daily) loading modes
   - Automatically runs all pipeline steps: data extraction → domain consolidation → DBT transformations
2. **DLT Pipeline** (`pipeline.py`): Extracts XLSX files from Dropbox folder and loads into PostgreSQL `raw` schema
3. **DBT Models**: Transform raw data through staging → intermediate → mart layers  
4. **Name Matching**: Domain consolidation normalizes company names during processing

### Schema Structure
- **Raw Schema**: Raw data loaded by DLT (customers, items, sales_receipts, invoices)
- **Staging Schema**: Cleaned and standardized data (`stg_quickbooks__*`)
- **Intermediate Schema**: Business logic transformations (`int_quickbooks__*`)
- **Mart Schema**: Final business-ready tables (`fct_*`)

### DBT Model Layers
- **Staging**: Basic cleaning, column selection, data type casting
- **Intermediate**: Business logic, joins, derived fields
- **Mart**: Final fact and dimension tables for consumption

### File Processing Logic
- Processes both backup files (`01_BACKUP_*`) and daily files
- Items use snapshot approach with `snapshot_date` for historical tracking
- Other tables use merge strategy with composite primary keys
- Name normalization applied to customer data via `matcher.py`

### Configuration
- **DBT Profile**: `profiles.yml` - PostgreSQL connection to `analytics` schema
- **DBT Project**: `dbt_project.yml` - Model materialization and schema configuration
- **Sources**: `models/sources.yml` - Defines raw data tables from `raw` schema

### Known Architectural Exceptions (DBT Project Evaluator)

This section documents intentionally accepted warnings/violations to avoid re-investigation:

#### Rejoining of Upstream Concepts (2 violations - ACCEPTED)
**Status**: Reduced from 7 to 2 violations (71% improvement) - remaining violations are business necessities

**Remaining Violations**:
- `mart_product_company_period_spending` → `fct_order_line_items` + `bridge_customer_company` + `fct_company_products`

**Business Justification**: 
- `mart_product_company_period_spending` requires transaction-level detail for period calculations (30d, 90d, 1y)
- Cannot be pre-aggregated due to dynamic date ranges and performance requirements
- `fct_company_products` provides lifetime metrics, while transaction details provide period-specific metrics
- Alternative architectures would require significant performance trade-offs or functionality loss

**Last Reviewed**: 2025-01-30 - Architectural decision to accept these as necessary for business requirements

## Important Files
- `pipeline.py`: Main DLT extraction logic
- `matcher.py`: Company name normalization
- `models/sources.yml`: DBT source definitions
- `dbt_project.yml`: DBT project configuration
- `profiles.yml`: Database connection settings
- `DBT_CANDIDATES.md`: Cross-project communication file tracking dashboard-driven pipeline improvements

## Current Schema Output

### Mart Tables (Dashboard Consumption)
- **`fct_orders`**: Order-level fact table with aggregated metrics, one row per order
  - Primary key: `order_number`
  - Contains: order metadata, customer info, financial totals, derived flags
  - **Status Standardization**: Clean, consistent status values (PAID, OPEN, PARTIALLY_PAID)
  - **Date Typing**: Proper DATE/TIMESTAMP types for all date fields
  - Used by dashboard for: order analytics, revenue tracking, customer insights

- **`fct_products`**: Product-level fact table with derived attributes, one row per product
  - Primary key: `item_name` (deduplicated by most recent record)
  - Contains: product details, categorization (product_family, material_type, is_kit), pricing
  - **Pricing Analytics**: Includes `sales_price`, `purchase_cost` (both NUMERIC), `margin_percentage`, `margin_amount`
  - Used by dashboard for: product catalog, inventory analytics, pricing insights, profit margin analysis

- **`fct_order_line_items`**: Line item-level fact table for invoice recreation, one row per line item
  - Primary key: `line_item_id` (unique DLT identifier)
  - Contains: complete order context, line item details, enriched product data, formatted addresses
  - **Invoice Recreation**: All fields needed to recreate customer invoices in frontend
  - **Product Enrichment**: Joined with `fct_products` for enhanced product details
  - **Clean Data Types**: Robust numeric parsing handles percentage values and data quality issues
  - Used by dashboard for: invoice display, line item analysis, detailed order breakdowns

## DBT Best Practices

### Model Organization
- Follow standard DBT layering: sources → staging → intermediate → mart
- Keep each model focused on a single, clear purpose
- Break down complex transformations into smaller, manageable models
- Refactor large or complex models into multiple smaller models

### Coding Standards
- Use clear, descriptive model and column names
- Include detailed comments for complex transformations
- Document models with descriptions in YAML files
- Use CTEs to break down complex logic into readable chunks
- Avoid trailing commas in SELECT statements
- Keep schema YAML files in sync with model changes

### Testing and Validation
- Verify data integrity between model layers with count checks
- Implement appropriate tests (unique, not_null, relationships)
- Validate that all orders and data points are preserved through transformations
- Always run `dbt test` after model changes

### Data Quality Improvements
- **Date Fields**: All date fields properly typed as DATE/TIMESTAMP, enabling native date operations
- **Status Standardization**: Order status values normalized to consistent uppercase format
- **Pricing Analytics**: Sales prices and purchase costs cast to NUMERIC with automatic margin calculations
- **Error Handling**: Robust NULL and empty string handling throughout the pipeline

### Performance and Data Integrity
- Use window functions (ROW_NUMBER()) for deduplication when necessary
- Ensure one row per business entity in fact tables
- Choose appropriate join keys (business vs. system IDs)
- Add uniqueness tests on business key columns
- Verify join conditions don't create Cartesian products
- Consider materialization strategies based on usage patterns

### Development Workflow
- Test queries against the database to validate transformations
- Compare record counts between source and target models
- Document assumptions and business rules in models
- When in doubt, refactor rather than extend complex models

## Memories
- dqi mcp is the output of this pipeline.
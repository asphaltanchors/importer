# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a QuickBooks data pipeline combining DLT (Data Loading Tool) for extraction and DBT (Data Build Tool) for transformation. The pipeline processes QuickBooks CSV exports (customers, items, sales receipts, invoices) from a Dropbox sync folder and loads them into PostgreSQL with transformations.

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
# Run the complete DLT pipeline
python pipeline.py

# Install dependencies
uv pip install -r requirements.txt
```

### DBT Commands
```bash
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
DROPBOX_PATH=/path/to/dropbox/folder
DATABASE_URL=postgresql://user:password@host:port/dbname
```

## Architecture

### Data Flow
1. **DLT Pipeline** (`pipeline.py`): Extracts CSV files from Dropbox folder and loads into PostgreSQL `raw` schema
2. **DBT Models**: Transform raw data through staging → intermediate → mart layers
3. **Name Matching**: `matcher.py` normalizes company names during extraction

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
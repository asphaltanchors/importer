# Multi-Source Data Pipeline with DBT

This project combines a DLT (Data Loading Tool) pipeline with DBT (Data Build Tool) transformations to process data from multiple e-commerce and accounting sources. Currently supports QuickBooks XLSX exports and Shopify e-commerce data, with a flexible multi-source architecture for future expansion.

**Multi-Source Architecture**: The pipeline supports both seed (historical) and incremental (daily) loading modes for efficient data processing across multiple data sources.

## Architecture

### Data Flow
1. **Orchestrator** (`orchestrator.py`): Master pipeline coordinator that runs the complete data workflow:
   - Supports `--seed` (historical) and `--incremental` (daily) loading modes
   - DLT extraction: Loads XLSX files from Dropbox into PostgreSQL `raw` schema
   - Domain consolidation: Creates `raw.domain_mapping` for company consolidation
   - DBT transformations: Processes data through staging → intermediate → mart layers
   - Multi-source architecture: Ready for future data source integrations
2. **Dashboard Integration**: Final `fct_*` tables feed into NextJS analytics dashboard

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

### Schema Structure
- **Raw Schema**: Raw data loaded by DLT
  - QuickBooks: customers, items, sales_receipts, invoices, domain_mapping
  - Shopify: customers, orders, products (+ nested tables for line_items, fulfillments, variants)
- **Staging Schema**: Cleaned and standardized data
  - QuickBooks: `stg_quickbooks__*`
  - Shopify: `stg_shopify__*`
- **Intermediate Schema**: Business logic transformations
  - QuickBooks: `int_quickbooks__*`
  - Shopify: `int_shopify__*`
  - Cross-system: `int_unified__*`
- **Mart Schema**: Final business-ready fact tables (`fct_*`, `mart_*`)

## Key Output Tables

### Core Fact Tables
- **`fct_orders`**: Order-level analytics with aggregated metrics (one row per order)
  - Enriched with Shopify attribution data for S- orders (marketing channels, UTM parameters, fulfillment tracking)
- **`fct_products`**: Product master with pricing analytics and margin calculations
- **`fct_order_line_items`**: Line item details for invoice recreation in frontend

### Company Analytics Tables
- **`fct_companies`**: Consolidated company master based on email domain consolidation
- **`fct_company_orders`**: Company-level order patterns and purchasing behavior
- **`fct_company_products`**: "Who buys what" analysis at company-product level
- **`bridge_customer_company`**: Links individual QuickBooks customers to consolidated companies

### Shopify Marketing Analytics Tables
- **`fct_order_attribution`**: Marketing attribution and channel performance (one row per Shopify order)
- **`fct_customer_marketing`**: Customer marketing engagement and segmentation
- **`mart_marketing_performance`**: Aggregated channel performance by month/campaign

### Features
- **Status Standardization**: Clean, consistent order status values (PAID, OPEN, PARTIALLY_PAID)
- **Pricing Analytics**: Automatic margin calculations (sales_price, purchase_cost, margin_percentage)
- **Company Consolidation**: Advanced domain-based customer consolidation reducing ~800 customer records to ~200 companies
- **Marketing Attribution**: UTM parameter tracking, acquisition channel classification, fulfillment tracking
- **Data Quality**: Proper date typing, robust NULL handling, comprehensive data validation
- **Cross-System Reconciliation**: Automated matching between Shopify and QuickBooks orders

## Setup

### Local Development

1. **Install dependencies:**
   ```bash
   uv pip install -r requirements.txt
   ```

2. **Environment variables** in `.env` file:
   ```bash
   DROPBOX_PATH=/path/to/your/dropbox/folder  # Parent directory containing seed/ and input/
   DATABASE_URL=postgresql://user:password@host:port/dbname
   ```

3. **Shopify configuration** in `.dlt/config.toml`:
   ```toml
   [sources.shopify_dlt]
   shop_url = "your-store.myshopify.com"
   private_app_password = "shppa_xxxxx"  # Admin API access token
   ```

4. **Run locally:**
   ```bash
   # Initial setup (load historical seed data)
   python pipeline.py --mode seed

   # Daily incremental loading (latest files only)
   python pipeline.py --mode incremental

   # Complete pipeline (seed + all incremental data)
   python pipeline.py --mode full

   # Run specific source pipelines
   python pipelines/shopify/pipeline.py --mode incremental

   # Optional: Run DBT commands separately (requires virtual environment)
   source .venv/bin/activate
   dbt run
   dbt test
   dbt docs generate && dbt docs serve

   # Run DBT project evaluator for best practices checks
   dbt run --select package:dbt_project_evaluator
   ```

### Docker/Production Deployment

This pipeline is designed to run as a containerized cron service alongside a dashboard application.

**Docker Compose Setup:**
```yaml
services:
  cron:
    build: 
      context: /path/to/importer
    environment:
      - DATABASE_URL=postgresql://postgres:postgres@db:5432/dashboard
    volumes:
      - dropbox_data:/dropbox
    depends_on:
      - db
    restart: unless-stopped
```

**Usage:**
```bash
# Normal operation: Runs incremental daily at midnight via cron
docker-compose up -d

# Initial setup (load historical seed data once)
docker-compose exec cron /app/entrypoint.sh seed

# Force incremental run (latest daily files only)
docker-compose exec cron /app/entrypoint.sh incremental

# Run complete pipeline (seed + all incremental)
docker-compose exec cron /app/entrypoint.sh full

# Run only DBT tests (useful after manual fixes)
docker-compose exec cron /app/entrypoint.sh test

# Interactive shell
docker-compose exec cron /app/entrypoint.sh shell
```

## Environment Configuration

The application uses different configurations for development vs production:

- **Local Development**: Uses `dev` DBT target with local database settings
- **Docker/Production**: Uses `prod` DBT target with containerized database settings

Environment variables:
- `DROPBOX_PATH`: Path to CSV files (default: `/dropbox/Dropbox/quickbooks-csv` in Docker)
- `DBT_TARGET`: DBT target to use (`dev` for local, `prod` for Docker)
- `DATABASE_URL`: Database connection string

## Running the Pipeline

### Local Development
```bash
# Complete pipeline (DLT + Domain Consolidation + DBT)
python pipeline.py

# Optional DBT commands (requires virtual environment)
source .venv/bin/activate
dbt test
dbt docs generate && dbt docs serve
```

### Docker/Production
```bash
# Monitor logs
docker-compose logs -f cron

# Run complete pipeline
docker-compose exec cron /app/entrypoint.sh seed
# or equivalently:
docker-compose exec cron /app/entrypoint.sh run

# Run tests only
docker-compose exec cron /app/entrypoint.sh test

# Debug mode
docker-compose exec cron /app/entrypoint.sh shell
```

### Company Consolidation
```bash
# Generate domain consolidation mapping (optional - automatically included in pipeline.py)
python domain_consolidation.py
```

## Key Files

- **`orchestrator.py`**: Multi-source pipeline orchestrator coordinating all data sources
- **`config/sources.yml`**: Source configuration for QuickBooks and Shopify
- **`pipelines/shopify/pipeline.py`**: Shopify DLT pipeline using verified source
- **`pipeline.py`**: QuickBooks DLT extraction logic
- **`matcher.py`**: Company name normalization utilities
- **`domain_consolidation.py`**: Email domain-based company consolidation
- **`models/staging/raw_data/sources.yml`**: DBT source definitions for raw tables
- **`dbt_project.yml`**: DBT project configuration with schema routing
- **`profiles.yml`**: Database connection settings
- **`DBT_CANDIDATES.md`**: Cross-project communication with dashboard team
- **`CLAUDE.md`**: Comprehensive project documentation and development guidelines
- **`MULTI_SOURCE_ARCHITECTURE.md`**: Multi-source pipeline design and implementation

## Data Processing Features

### File Processing Logic
- Processes both backup files (`01_BACKUP_*`) and daily files
- Items use snapshot approach with `snapshot_date` for historical tracking
- Other tables use merge strategy with composite primary keys
- Advanced company name normalization via `matcher.py`

### Company Consolidation System
- Email domain-based customer consolidation
- Reduces ~800 individual customer records to ~200 consolidated companies
- Supports corporate domains, individual customers, and businesses without email
- Enables "who really buys from us" analysis beyond messy QuickBooks customer names

### Data Quality Improvements
- **Proper Date Typing**: All date fields cast to DATE/TIMESTAMP types
- **Status Standardization**: Consistent uppercase status values
- **Pricing Analytics**: Automatic margin calculations with NUMERIC typing
- **Robust Error Handling**: NULL and empty string handling throughout pipeline
- **Cross-System Validation**: Automated reconciliation between Shopify and QuickBooks orders via `int_unified__order_matching`

### Shopify Integration
- **DLT Verified Source**: Uses official DLT Shopify source for reliable data extraction
- **Marketing Attribution**: Extracts UTM parameters, acquisition channels, landing pages, and referral sources
- **Fulfillment Tracking**: Captures tracking numbers, shipping status, and fulfillment dates
- **Customer Engagement**: Tracks email/SMS marketing consent and customer behavior data
- **Discount Analytics**: Analyzes discount usage patterns and promotional effectiveness
- **Data Reconciliation**: 100% match validation between Shopify orders (S- prefix) and QuickBooks records

## Project Structure

```
.
├── .env                        # Environment variables (not in repo)
├── .dlt/                       # DLT configuration directory
│   ├── config.toml            # Source credentials (Shopify API tokens)
│   └── secrets.toml           # Sensitive configuration (gitignored)
├── README.md                   # This file
├── CLAUDE.md                   # Project documentation and development guide
├── DBT_CANDIDATES.md           # Dashboard-pipeline communication
├── MULTI_SOURCE_ARCHITECTURE.md # Multi-source design documentation
├── dbt_project.yml             # DBT project configuration
├── profiles.yml                # DBT connection profiles
├── orchestrator.py             # Multi-source pipeline orchestrator
├── pipeline.py                 # QuickBooks DLT pipeline
├── matcher.py                  # Company name normalization logic
├── domain_consolidation.py     # Company consolidation system
├── config/
│   └── sources.yml            # Source configuration (QuickBooks, Shopify)
├── pipelines/
│   └── shopify/
│       ├── pipeline.py        # Shopify DLT pipeline
│       └── config.yml         # Shopify pipeline documentation
├── models/
│   ├── staging/
│   │   ├── raw_data/sources.yml  # DBT sources definition
│   │   ├── quickbooks/        # QuickBooks staging models
│   │   └── shopify/           # Shopify staging models
│   ├── intermediate/
│   │   ├── quickbooks/        # QuickBooks business logic
│   │   └── shopify/           # Shopify enrichment + unified matching
│   └── mart/                  # Final fact tables for consumption
├── requirements.txt            # Python dependencies
└── target/                     # DBT build artifacts
```

## Dashboard Integration

This pipeline feeds data to a NextJS dashboard application. The `fct_*` tables in the mart schema are the final output tables directly consumed by the dashboard for:
- Order analytics and revenue tracking
- Product catalog and inventory insights
- Customer/company analytics with consolidated views
- Invoice recreation and detailed reporting
- Marketing attribution and campaign performance analysis
- Customer engagement and segmentation

The `DBT_CANDIDATES.md` file serves as communication channel between this pipeline and the dashboard team for tracking data quality improvements and optimization opportunities.

## DBT Best Practices Validation

This project uses the [DBT Project Evaluator](https://github.com/dbt-labs/dbt-project-evaluator) package to enforce best practices. Run the evaluator to check for:
- Proper model layering (staging → intermediate → mart)
- No circular dependencies or rejoining of upstream concepts
- Appropriate materialization strategies
- Documentation coverage
- Testing coverage

```bash
# Activate virtual environment
source .venv/bin/activate

# Run the DBT project evaluator
dbt run --select package:dbt_project_evaluator

# View results in dbt_project_evaluator schema
psql $DATABASE_URL -c "SELECT * FROM analytics.dbt_project_evaluator.fct_documentation_coverage;"
```

The evaluator should pass all checks. Any intentional violations are documented in `CLAUDE.md` under "Known Architectural Exceptions".

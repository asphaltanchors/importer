# QuickBooks Data Pipeline with DBT

This project combines a DLT (Data Loading Tool) pipeline with DBT (Data Build Tool) transformations to process QuickBooks CSV exports and create analytics-ready datasets. The pipeline processes data from customers, items, sales receipts, and invoices, applying company name normalization and creating comprehensive fact tables for business intelligence.

## Architecture

### Data Flow
1. **Complete Pipeline** (`pipeline.py`): Orchestrates the complete data workflow:
   - DLT extraction: Loads CSV files from Dropbox into PostgreSQL `raw` schema
   - Domain consolidation: Creates `raw.domain_mapping` for company consolidation
   - DBT transformations: Processes data through staging → intermediate → mart layers
2. **Dashboard Integration**: Final `fct_*` tables feed into NextJS analytics dashboard

### Schema Structure
- **Raw Schema**: Raw data loaded by DLT (customers, items, sales_receipts, invoices, domain_mapping)
- **Staging Schema**: Cleaned and standardized data (`stg_quickbooks__*`)
- **Intermediate Schema**: Business logic transformations (`int_quickbooks__*`)
- **Mart Schema**: Final business-ready fact tables (`fct_*`)

## Key Output Tables

### Core Fact Tables
- **`fct_orders`**: Order-level analytics with aggregated metrics (one row per order)
- **`fct_products`**: Product master with pricing analytics and margin calculations
- **`fct_order_line_items`**: Line item details for invoice recreation in frontend

### Company Analytics Tables
- **`fct_companies`**: Consolidated company master based on email domain consolidation
- **`fct_company_orders`**: Company-level order patterns and purchasing behavior
- **`fct_company_products`**: "Who buys what" analysis at company-product level
- **`bridge_customer_company`**: Links individual QuickBooks customers to consolidated companies

### Features
- **Status Standardization**: Clean, consistent order status values (PAID, OPEN, PARTIALLY_PAID)
- **Pricing Analytics**: Automatic margin calculations (sales_price, purchase_cost, margin_percentage)
- **Company Consolidation**: Advanced domain-based customer consolidation reducing ~800 customer records to ~200 companies
- **Data Quality**: Proper date typing, robust NULL handling, comprehensive data validation

## Setup

### Local Development

1. **Install dependencies:**
   ```bash
   uv pip install -r requirements.txt
   ```

2. **Environment variables** in `.env` file:
   ```bash
   DROPBOX_PATH=/path/to/your/dropbox/folder
   DATABASE_URL=postgresql://user:password@host:port/dbname
   ```

3. **Run locally:**
   ```bash
   # Complete pipeline (DLT + Domain Consolidation + DBT)
   python pipeline.py
   
   # Optional: Run DBT commands separately (requires virtual environment)
   source .venv/bin/activate
   dbt test
   dbt docs generate && dbt docs serve
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
# Normal operation: Runs daily at midnight via cron
docker-compose up -d

# Run complete pipeline (DLT + Domain Consolidation + DBT)
docker-compose exec cron /app/entrypoint.sh seed
# or equivalently:
docker-compose exec cron /app/entrypoint.sh run

# Run only DBT tests (useful after manual fixes)
docker-compose exec cron /app/entrypoint.sh test

# Interactive shell
docker-compose exec cron /app/entrypoint.sh shell

# Direct pipeline execution
docker-compose exec cron python pipeline.py
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

- **`pipeline.py`**: Complete pipeline orchestrator (DLT extraction + Domain consolidation + DBT transformations)
- **`matcher.py`**: Company name normalization utilities
- **`domain_consolidation.py`**: Email domain-based company consolidation
- **`models/sources.yml`**: DBT source definitions
- **`dbt_project.yml`**: DBT project configuration with schema routing
- **`profiles.yml`**: Database connection settings
- **`DBT_CANDIDATES.md`**: Cross-project communication with dashboard team
- **`CLAUDE.md`**: Comprehensive project documentation and development guidelines

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

## Project Structure

```
.
├── .env                     # Environment variables (not in repo)
├── README.md                # This file
├── CLAUDE.md                # Project documentation and development guide
├── DBT_CANDIDATES.md        # Dashboard-pipeline communication
├── dbt_project.yml          # DBT project configuration
├── profiles.yml             # DBT connection profiles
├── pipeline.py              # Main DLT pipeline script
├── matcher.py               # Company name normalization logic
├── domain_consolidation.py  # Company consolidation system
├── models/
│   ├── sources.yml          # DBT sources definition
│   ├── staging/             # Data cleaning and standardization
│   ├── intermediate/        # Business logic transformations
│   └── mart/                # Final fact tables for consumption
├── requirements.txt         # Python dependencies
└── target/                  # DBT build artifacts
```

## Dashboard Integration

This pipeline feeds data to a NextJS dashboard application. The `fct_*` tables in the mart schema are the final output tables directly consumed by the dashboard for:
- Order analytics and revenue tracking
- Product catalog and inventory insights  
- Customer/company analytics with consolidated views
- Invoice recreation and detailed reporting

The `DBT_CANDIDATES.md` file serves as communication channel between this pipeline and the dashboard team for tracking data quality improvements and optimization opportunities.

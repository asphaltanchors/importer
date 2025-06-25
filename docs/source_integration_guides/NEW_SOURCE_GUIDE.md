# Adding a New Data Source - Integration Guide

This guide walks through adding a new data source to the multi-source pipeline architecture.

## Overview

The pipeline supports multiple data sources feeding into a unified raw schema, then processed through DBT transformations. Each source is independent and can be developed, tested, and deployed separately.

## Step-by-Step Integration Process

### 1. Create Source Directory Structure

```bash
mkdir pipelines/{source_name}
cd pipelines/{source_name}
```

### 2. Copy Template Files

```bash
# Copy the pipeline template
cp ../template_pipeline.py pipeline.py

# Create source configuration
cat > config.yml << EOF
# {Source Name} Pipeline Configuration
source_name: "{source_name}"
description: "{Source Name} data extraction and processing"

# DLT configuration
dlt_pipeline_name: "dqi"
dlt_dataset_name: "raw"

# API/Source configuration (customize as needed)
# api_base_url: "https://api.example.com"
# rate_limit_requests_per_minute: 60

# Scheduling
schedule: "hourly"  # or "daily"
priority: 2  # execution order (1 = first)
EOF
```

### 3. Customize the Pipeline

Edit `pipeline.py` and make these changes:

#### Replace Template Names
- `CHANGE_ME_SOURCE_NAME` → `{source_name}` (e.g., "attio", "shopify")
- `template_source` → `{source_name}_source`
- `extract_template_data` → `extract_{table_name}`
- `template_table` → actual table name

#### Add Environment Variables
```python
REQUIRED_ENV_VARS = [
    "DATABASE_URL",
    "{SOURCE_NAME}_API_KEY",      # Add actual required vars
    "{SOURCE_NAME}_BASE_URL",
    # ...
]
```

#### Implement Data Extraction
Replace the sample data block with actual extraction logic:

```python
@dlt.resource(
    write_disposition="merge",
    name="contacts",  # actual table name
    primary_key=["id"]
)
def extract_contacts():
    """Extract contacts from API"""
    logger.info("Starting contacts extraction")
    
    # Your API calls here
    response = api_client.get("/contacts")
    
    for contact in response["data"]:
        yield {
            **contact,
            "load_date": datetime.utcnow().date().isoformat(),
            "source": "{source_name}"
        }
```

### 4. Add Source to Configuration

Edit `config/sources.yml`:

```yaml
sources:
  {source_name}:
    enabled: true
    schedule: "hourly"
    path: "pipelines/{source_name}"
    priority: 2
    description: "{Source Name} data via API"
    tables:
      - "contacts"
      - "companies" 
      - "deals"
    data_quality:
      check_freshness: true
      max_age_hours: 2
      required_columns:
        contacts: ["id", "email"]
        companies: ["id", "name"]
```

### 5. Create DBT Models

#### Add Source Definition
Create or update `models/sources/{source_name}.yml`:

```yaml
version: 2

sources:
  - name: {source_name}
    description: "{Source Name} raw data"
    schema: raw
    tables:
      - name: contacts
        description: "Contact records from {Source Name}"
        columns:
          - name: id
            description: "Unique contact identifier"
            tests:
              - unique
              - not_null
```

#### Create Staging Models
Create `models/staging/{source_name}/stg_{source_name}__contacts.sql`:

```sql
-- ABOUTME: Staging model for {source_name} contacts
-- ABOUTME: Basic cleaning and standardization of raw contact data

{{ config(materialized='view') }}

select
    id,
    email,
    first_name,
    last_name,
    company_id,
    created_at,
    updated_at,
    load_date,
    'contacts' as source_table,
    '{source_name}' as source_system
    
from {{ source('{source_name}', 'contacts') }}
where id is not null
```

### 6. Environment Setup

Add required environment variables to `.env`:

```bash
# {Source Name} Configuration
{SOURCE_NAME}_API_KEY=your_api_key_here
{SOURCE_NAME}_BASE_URL=https://api.example.com/v1
```

### 7. Test the Integration

#### Test Pipeline Individually
```bash
cd pipelines/{source_name}
source ../../.venv/bin/activate
python pipeline.py
```

#### Test via Orchestrator
```bash
source .venv/bin/activate
python orchestrator.py --mode source --source {source_name}
```

#### Test DBT Models
```bash
source .venv/bin/activate
dbt run --select staging.{source_name}
dbt test --select staging.{source_name}
```

### 8. Integration Testing

#### Test Full Pipeline
```bash
python orchestrator.py --mode full
```

#### Verify Data Quality
```bash
python orchestrator.py --mode data-quality
```

## Common Integration Patterns

### API-Based Sources (Attio, Shopify)

```python
# Add API client configuration
class ApiClient:
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        })
    
    def get_paginated(self, endpoint: str):
        """Handle paginated API responses"""
        url = f"{self.base_url}{endpoint}"
        
        while url:
            response = self.session.get(url)
            response.raise_for_status()
            
            data = response.json()
            yield from data.get("data", [])
            
            url = data.get("next_page_url")
```

### File-Based Sources (Google Analytics exports)

```python
@dlt.resource(name="ga_sessions")
def extract_ga_sessions():
    """Extract from GA CSV exports"""
    
    for file_path in glob.glob("/path/to/ga_exports/*.csv"):
        with open(file_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield {
                    **row,
                    "file_source": os.path.basename(file_path),
                    "load_date": datetime.utcnow().date().isoformat()
                }
```

### Database Sources (External APIs with SQL)

```python
@dlt.resource(name="external_orders")
def extract_external_orders():
    """Extract from external database"""
    
    query = """
    SELECT * FROM orders 
    WHERE updated_at >= %s
    """
    
    with external_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, (last_sync_date,))
        
        for row in cursor.fetchall():
            yield dict(row)
```

## DBT Integration Patterns

### Cross-Source Customer Unification

Create intermediate models that join across sources:

```sql
-- models/intermediate/customer_unification/int_unified_customers.sql

with quickbooks_customers as (
    select * from {{ ref('stg_quickbooks__customers') }}
),

{source_name}_contacts as (
    select * from {{ ref('stg_{source_name}__contacts') }}
),

unified as (
    select
        coalesce(qb.email, sc.email) as email,
        coalesce(qb.customer_name, sc.full_name) as customer_name,
        qb.quickbooks_id,
        sc.{source_name}_id,
        'quickbooks' as primary_source
    from quickbooks_customers qb
    full outer join {source_name}_contacts sc
        on lower(qb.email) = lower(sc.email)
)

select * from unified
```

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure shared utilities path is correct
2. **Database Connection**: Verify DATABASE_URL is accessible from subdirectory
3. **API Rate Limits**: Implement proper rate limiting and retry logic
4. **Large Data Volumes**: Use DLT's incremental loading features

### Debug Mode

Run with verbose logging:

```python
logger = setup_logging("{source_name}", "DEBUG")
```

### Testing Individual Components

```python
# Test just the extraction
if __name__ == "__main__":
    env_vars = validate_environment()
    
    # Test extraction without DLT
    for record in extract_contacts():
        print(record)
```

## Next Steps

1. Add data quality tests specific to your source
2. Create mart-level models that combine this source with others
3. Add monitoring and alerting for source-specific issues
4. Document source-specific business logic and transformations

## Source-Specific Guides

- [Attio CRM Integration](./ATTIO_INTEGRATION.md) *(coming soon)*
- [Google Analytics Integration](./GA_INTEGRATION.md) *(coming soon)*
- [Shopify Integration](./SHOPIFY_INTEGRATION.md) *(coming soon)*
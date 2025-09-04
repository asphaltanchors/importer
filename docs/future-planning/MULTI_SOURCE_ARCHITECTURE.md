# Multi-Source Data Pipeline Architecture Plan

## Current State Analysis

### Existing Architecture (Working Well)
- **DLT Pipeline**: QuickBooks CSV extraction → PostgreSQL raw schema
- **Custom Processing**: `domain_consolidation.py` for name matching/normalization
- **DBT Transformations**: staging → intermediate → mart layers
- **Dashboard Integration**: `fct_*` tables consumed by NextJS dashboard
- **Scheduling**: Basic cron job execution

### Current Data Sources
1. **QuickBooks CSV**: customers, items, sales_receipts, invoices
2. **CoreSignal**: Company enrichment data (JSONL)

## Target Architecture

### Design Principles for Solo Development
- **Simplicity**: Cron-based scheduling, no complex orchestrators
- **Modularity**: Each data source is independent
- **Maintainability**: Clear file organization and documentation
- **Scalability**: Easy to add new sources without refactoring existing ones
- **Data Quality**: Automated monitoring and validation

### Planned Data Sources
1. **QuickBooks** (existing) - Financial/customer data
2. **Attio CRM** - Customer relationship management
3. **Google Analytics** - Website traffic and user behavior
4. **Shopify** - E-commerce transactions and products

## Implementation Roadmap

### Phase 1: Refactor Current Pipeline (1-2 weeks)

#### File Structure Reorganization
```
importer/
├── pipelines/
│   ├── quickbooks/
│   │   ├── pipeline.py
│   │   ├── domain_consolidation.py
│   │   └── config.yml
│   ├── attio/
│   │   ├── pipeline.py
│   │   ├── api_client.py
│   │   └── config.yml
│   ├── google_analytics/
│   │   ├── pipeline.py
│   │   ├── ga_client.py
│   │   └── config.yml
│   ├── shopify/
│   │   ├── pipeline.py
│   │   ├── shopify_client.py
│   │   └── config.yml
│   └── shared/
│       ├── utils.py
│       ├── data_quality.py
│       └── database.py
├── models/ (DBT - enhanced structure)
├── orchestrator.py (Master pipeline runner)
├── config/
│   ├── sources.yml
│   └── scheduling.yml
└── docs/
    └── source_integration_guides/
```

#### Migration Steps
1. Create new directory structure
2. Move existing QuickBooks code to `pipelines/quickbooks/`
3. Extract shared utilities to `pipelines/shared/`
4. Create master orchestrator script
5. Update cron jobs to use orchestrator
6. Test existing pipeline functionality

### Phase 2: Enhanced DBT Architecture

#### New DBT Structure
```
models/
├── sources/
│   ├── quickbooks.yml
│   ├── attio.yml
│   ├── google_analytics.yml
│   └── shopify.yml
├── staging/
│   ├── quickbooks/
│   │   ├── stg_quickbooks__customers.sql
│   │   ├── stg_quickbooks__items.sql
│   │   └── ...
│   ├── attio/
│   │   ├── stg_attio__contacts.sql
│   │   ├── stg_attio__companies.sql
│   │   └── ...
│   ├── google_analytics/
│   │   ├── stg_ga__sessions.sql
│   │   ├── stg_ga__events.sql
│   │   └── ...
│   └── shopify/
│       ├── stg_shopify__orders.sql
│       ├── stg_shopify__products.sql
│       └── ...
├── intermediate/
│   ├── customer_unification/
│   │   ├── int_unified_customers.sql
│   │   └── int_customer_mapping.sql
│   ├── product_matching/
│   │   ├── int_unified_products.sql
│   │   └── int_product_mapping.sql
│   └── revenue_attribution/
│       ├── int_revenue_by_source.sql
│       └── int_customer_journey.sql
└── mart/
    ├── customers/
    │   ├── fct_customers_unified.sql
    │   └── dim_customer_segments.sql
    ├── products/
    │   ├── fct_products_unified.sql
    │   └── dim_product_categories.sql
    └── analytics/
        ├── fct_revenue_attribution.sql
        └── fct_customer_lifetime_value.sql
```

### Phase 3: Add Attio CRM Integration (2-3 weeks)

#### Attio Integration Plan
- **Data Sources**: contacts, companies, deals, activities
- **API Approach**: OpenAPI spec → generated client
- **Raw Schema**: `raw.attio__contacts`, `raw.attio__companies`, etc.
- **Key Integrations**: 
  - Customer unification with QuickBooks customers
  - Company enrichment beyond CoreSignal
  - Sales pipeline analytics

#### Implementation Steps
1. Generate Attio API client from OpenAPI spec
2. Create `pipelines/attio/pipeline.py` following QuickBooks pattern
3. Add Attio staging models in DBT
4. Create customer unification logic in intermediate layer
5. Update mart models to include CRM data

### Phase 4: Add Google Analytics Integration (2-3 weeks)

#### GA Integration Plan
- **Data Sources**: sessions, events, conversions, audience data
- **API Approach**: GA4 Data API + Reporting API
- **Raw Schema**: `raw.ga__sessions`, `raw.ga__events`, etc.
- **Key Integrations**:
  - Website traffic attribution to customers
  - Product page analytics to inventory
  - Conversion funnel analysis

#### Implementation Steps
1. Set up GA4 API credentials and client
2. Create `pipelines/google_analytics/pipeline.py` 
3. Add GA staging models in DBT
4. Create attribution logic linking web traffic to customers
5. Build analytics models for website performance

### Phase 5: Add Shopify Integration (2-3 weeks)

#### Shopify Integration Plan
- **Data Sources**: orders, products, customers, inventory
- **API Approach**: Shopify REST Admin API + webhooks
- **Raw Schema**: `raw.shopify__orders`, `raw.shopify__products`, etc.
- **Key Integrations**:
  - E-commerce revenue vs. direct sales
  - Product catalog synchronization
  - Customer behavior across channels

#### Implementation Steps
1. Set up Shopify API credentials and webhooks
2. Create `pipelines/shopify/pipeline.py`
3. Add Shopify staging models in DBT
4. Create product matching logic between Shopify and QuickBooks
5. Build omnichannel analytics models

## Technical Implementation Details

### Master Orchestrator Pattern

```python
# orchestrator.py
"""
ABOUTME: Master pipeline orchestrator for multi-source data ingestion
ABOUTME: Runs individual source pipelines based on configuration and scheduling
"""

class PipelineOrchestrator:
    def __init__(self, config_path: str):
        self.config = load_config(config_path)
        self.logger = setup_logging()
    
    def run_source_pipeline(self, source_name: str):
        """Run individual source pipeline"""
        # Import and execute source-specific pipeline
        # Handle errors and logging
        # Update pipeline status
    
    def run_dbt_transformations(self):
        """Run DBT transformations after all sources complete"""
        # Run dbt run with proper dependency management
    
    def run_full_pipeline(self):
        """Run complete pipeline in correct order"""
        # 1. Run all source pipelines
        # 2. Run DBT transformations
        # 3. Run data quality checks
```

### Cron Scheduling Strategy

```bash
# /etc/crontab or user crontab

# Daily full pipeline at 2 AM
0 2 * * * cd /path/to/importer && python orchestrator.py --full

# Hourly incremental updates for real-time sources
0 * * * * cd /path/to/importer && python orchestrator.py --incremental

# Weekly data quality reports
0 6 * * 1 cd /path/to/importer && python orchestrator.py --data-quality-report
```

### Source Pipeline Template

```python
# pipelines/template/pipeline.py
"""
ABOUTME: Template for new data source pipeline integration
ABOUTME: Copy and modify this template for each new data source
"""

@dlt.source
def source_name_source():
    @dlt.resource(
        write_disposition="merge",
        name="table_name",
        primary_key=["id"]
    )
    def extract_table():
        # Source-specific extraction logic
        pass
    
    return [extract_table]

def run_pipeline():
    """Main pipeline execution function"""
    pipeline = dlt.pipeline(
        pipeline_name=f"source_name_pipeline",
        destination="postgres", 
        dataset_name="raw"
    )
    
    load_info = pipeline.run(source_name_source())
    return load_info

if __name__ == "__main__":
    run_pipeline()
```

### Data Quality Monitoring

#### Automated Checks
- **Row Count Validation**: Compare source vs. raw table counts
- **Schema Drift Detection**: Monitor for unexpected column changes
- **Data Freshness**: Alert on stale data beyond expected intervals
- **Cross-Source Consistency**: Validate customer/product matching

#### Monitoring Tools (Lightweight)
- **DBT Tests**: Built-in data quality checks
- **Custom Python Scripts**: Source-specific validation
- **Database Triggers**: Real-time constraint monitoring
- **Log Analysis**: Pipeline failure detection

## Data Integration Patterns

### Customer Unification Strategy
1. **Primary Keys**: Email, phone, company domain
2. **Fuzzy Matching**: Company name normalization (existing)
3. **Manual Override**: Configuration file for complex cases
4. **Confidence Scoring**: Rate match quality for review

### Product Catalog Synchronization
1. **SKU Matching**: Primary product identifier
2. **Name Normalization**: Similar to customer name matching
3. **Category Mapping**: Cross-platform category standardization
4. **Inventory Reconciliation**: Multi-source stock level tracking

### Revenue Attribution Model
1. **Source Tagging**: Track revenue origin (QB, Shopify, etc.)
2. **Customer Journey**: Multi-touchpoint attribution
3. **Channel Performance**: Compare sales channel effectiveness
4. **Time-based Analysis**: Revenue trends across sources

## Migration Guide

### Step-by-Step Migration from Current Setup

#### Week 1: Structure Setup
1. Create new directory structure
2. Move existing files to new locations
3. Update import paths
4. Test existing functionality

#### Week 2: Orchestrator Implementation
1. Create master orchestrator script
2. Convert existing cron jobs
3. Add logging and error handling
4. Test full pipeline execution

#### Week 3+: Source Addition (per source)
1. Research API/data source requirements
2. Implement extraction pipeline
3. Create DBT staging models
4. Add to orchestrator configuration
5. Test integration and data quality

## Configuration Management

### Environment Variables
```bash
# .env file structure
DATABASE_URL=postgresql://...
DROPBOX_PATH=/path/to/dropbox

# Attio
ATTIO_API_KEY=...
ATTIO_BASE_URL=...

# Google Analytics  
GA_SERVICE_ACCOUNT_PATH=...
GA_PROPERTY_ID=...

# Shopify
SHOPIFY_API_KEY=...
SHOPIFY_API_SECRET=...
SHOPIFY_SHOP_DOMAIN=...
```

### Source Configuration Files
```yaml
# config/sources.yml
sources:
  quickbooks:
    enabled: true
    schedule: "daily"
    path: "pipelines/quickbooks"
  
  attio:  
    enabled: true
    schedule: "hourly"
    path: "pipelines/attio"
    
  google_analytics:
    enabled: true
    schedule: "daily"
    path: "pipelines/google_analytics"
    
  shopify:
    enabled: true  
    schedule: "hourly"
    path: "pipelines/shopify"
```

## Success Metrics

### Technical Metrics
- **Pipeline Reliability**: >99% successful runs
- **Data Freshness**: <2 hour lag for real-time sources
- **Data Quality**: <1% error rate in transformations
- **Performance**: <30 minute full pipeline execution

### Business Metrics
- **Customer 360**: Single view across all sources
- **Revenue Attribution**: Track sales by channel/source
- **Product Performance**: Cross-platform inventory insights
- **Customer Journey**: Multi-touchpoint analytics

## Maintenance & Operations

### Daily Operations
- Monitor pipeline execution logs
- Review data quality alerts
- Check source API status/limits

### Weekly Operations  
- Review error patterns and resolution
- Update source configurations as needed
- Performance optimization

### Monthly Operations
- Source API usage analysis
- Data model performance review
- Documentation updates

## Risk Mitigation

### Common Risks & Solutions
1. **API Rate Limits**: Implement backoff strategies and caching
2. **Schema Changes**: Version control and backward compatibility
3. **Data Volume Growth**: Incremental extraction patterns
4. **Source Downtime**: Graceful degradation and retry logic
5. **Solo Developer Dependency**: Comprehensive documentation and automation

This architecture plan provides a robust foundation for scaling your data pipeline while maintaining simplicity and reliability for solo development.
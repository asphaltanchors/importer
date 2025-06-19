# Company Data Enrichment Integration Plan

## Overview
This plan outlines how to integrate external company data enrichment (revenue, employees, description, logo, etc.) into the existing DQI pipeline while leveraging already-enriched data to minimize API costs.

## Current State Analysis
- Mature company consolidation system using email domains (`fct_companies.sql`)
- Companies identified and consolidated by normalized email domains  
- Pipeline extracts primary company info (name, address, phone, revenue metrics)
- **100+ companies already enriched via API** - opportunity for one-time CSV load

## Implementation Strategy

### Phase 1: One-Time CSV Load (Immediate Value)

#### 1.1 Prepare Enriched Data
- **Input**: Your existing enriched company CSV file
- **Required Fields**: 
  - `company_domain` (for matching to existing companies)
  - `enriched_revenue` (annual revenue)
  - `employee_count` 
  - `company_description`
  - `logo_url`
  - `industry`
  - `founded_year`
  - `enrichment_source` (API provider name)
  - `enrichment_date` (when data was retrieved)

#### 1.2 Create DLT Resource for CSV Load
```python
# Add to pipeline.py
@dlt.resource(
    write_disposition="merge",
    name="company_enrichment",
    primary_key=["company_domain"]
)
def extract_company_enrichment():
    """One-time load of pre-enriched company data"""
    enrichment_file = os.path.join(DROPBOX_PATH, "company_enrichment.csv")
    if os.path.exists(enrichment_file):
        with open(enrichment_file, newline="") as fh:
            rdr = csv.DictReader(fh)
            for row in rdr:
                yield {
                    **row,
                    "load_date": datetime.utcnow().date().isoformat(),
                    "is_manual_load": True
                }
```

#### 1.3 Create Staging Model
```sql
-- models/staging/stg_quickbooks__company_enrichment.sql
{{ config(materialized = 'view') }}

SELECT 
    company_domain,
    CAST(NULLIF(TRIM(enriched_revenue), '') AS NUMERIC) as annual_revenue,
    CAST(NULLIF(TRIM(employee_count), '') AS INTEGER) as employee_count,
    NULLIF(TRIM(company_description), '') as description,
    NULLIF(TRIM(logo_url), '') as logo_url,
    NULLIF(TRIM(industry), '') as industry,
    CAST(NULLIF(TRIM(founded_year), '') AS INTEGER) as founded_year,
    NULLIF(TRIM(enrichment_source), '') as enrichment_source,
    CAST(enrichment_date AS DATE) as enrichment_date,
    load_date
FROM {{ source('raw_data', 'company_enrichment') }}
WHERE company_domain IS NOT NULL 
  AND TRIM(company_domain) != ''
```

#### 1.4 Enhance Company Fact Table
```sql
-- Update models/mart/fct_companies.sql
-- Add LEFT JOIN to enrichment data in the final SELECT:

LEFT JOIN {{ ref('stg_quickbooks__company_enrichment') }} ce 
    ON cf.company_domain_key = ce.company_domain

-- Add enrichment fields to SELECT:
    ce.annual_revenue as enriched_annual_revenue,
    ce.employee_count as enriched_employee_count,
    ce.description as enriched_description,
    ce.logo_url as enriched_logo_url,
    ce.industry as enriched_industry,
    ce.founded_year as enriched_founded_year,
    ce.enrichment_source,
    ce.enrichment_date,
```

### Phase 2: API Service Foundation (Future Implementation)

#### 2.1 Company Enrichment Service
```python
# company_enricher.py
import requests
from typing import Optional, Dict
import time

class CompanyEnricher:
    def __init__(self, api_key: str, api_base_url: str):
        self.api_key = api_key
        self.api_base_url = api_base_url
        self.rate_limit_delay = 1.0  # seconds between requests
        
    def enrich_company(self, domain: str) -> Optional[Dict]:
        """Enrich company data via API"""
        try:
            time.sleep(self.rate_limit_delay)  # Rate limiting
            response = requests.get(
                f"{self.api_base_url}/company/{domain}",
                headers={"Authorization": f"Bearer {self.api_key}"},
                timeout=30
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"API error for {domain}: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"Enrichment failed for {domain}: {e}")
            return None
            
    def should_enrich(self, domain: str, last_enriched: Optional[str]) -> bool:
        """Check if company needs enrichment (not in existing data)"""
        # Logic to check if company already enriched
        # Could query database or check local cache
        return True  # Implement based on needs
```

#### 2.2 Incremental Pipeline Resource
```python
# Add to pipeline.py for future use
@dlt.resource(
    write_disposition="merge", 
    name="company_enrichment_api",
    primary_key=["company_domain"]
)
def extract_new_company_enrichment():
    """Enrich new companies via API"""
    enricher = CompanyEnricher(
        api_key=os.environ.get("ENRICHMENT_API_KEY"),
        api_base_url=os.environ.get("ENRICHMENT_API_URL")
    )
    
    # Get companies that need enrichment
    # (would query existing companies not in enrichment table)
    companies_to_enrich = get_unenriched_companies()
    
    for domain in companies_to_enrich:
        enrichment_data = enricher.enrich_company(domain)
        if enrichment_data:
            yield {
                "company_domain": domain,
                **enrichment_data,
                "load_date": datetime.utcnow().date().isoformat(),
                "is_manual_load": False
            }
```

### Phase 3: Automated Pipeline (Future Enhancement)

#### 3.1 Scheduled Enrichment
- Add cron job or scheduled task to run enrichment pipeline
- Enrich companies quarterly or when new companies appear
- Monitor API usage and costs

#### 3.2 Data Quality Monitoring  
- Track enrichment coverage rates
- Monitor API response quality
- Alert on enrichment failures

## Environment Configuration

### Required Environment Variables
```bash
# Add to .env file when implementing API phase
ENRICHMENT_API_KEY=your_api_key_here
ENRICHMENT_API_URL=https://api.provider.com/v1
ENRICHMENT_RATE_LIMIT=1.0  # seconds between requests
```

### Dependencies to Add
```txt
# Add to requirements.txt when implementing API phase
requests>=2.28.0
```

## Benefits

### Immediate (Phase 1)
- Enhanced company profiles with revenue, employee count, industry data
- Better customer segmentation and analytics
- Leverages existing API investment without additional costs
- Maintains existing consolidation logic

### Future (Phases 2-3)  
- Automated enrichment for new companies
- Scalable enrichment pipeline
- Cost-effective API usage through intelligent caching
- Foundation for advanced company analytics

## Implementation Checklist

### Phase 1 - CSV Load
- [ ] Prepare enriched company CSV with required schema
- [ ] Add company_enrichment resource to pipeline.py  
- [ ] Create stg_quickbooks__company_enrichment.sql
- [ ] Update fct_companies.sql with enrichment JOIN
- [ ] Test pipeline with enriched data
- [ ] Validate enrichment data appears in company fact table

### Phase 2 - API Service (Future)
- [ ] Implement CompanyEnricher class
- [ ] Add environment configuration
- [ ] Create incremental enrichment resource
- [ ] Test API integration
- [ ] Implement rate limiting and error handling

### Phase 3 - Automation (Future)  
- [ ] Schedule regular enrichment runs
- [ ] Add monitoring and alerting
- [ ] Optimize API usage patterns
- [ ] Document operational procedures

## Notes
- This approach maximizes existing investment in enriched data
- Provides immediate value while building foundation for automation
- Maintains backward compatibility with existing pipeline
- Designed for cost-effective scaling of enrichment capabilities
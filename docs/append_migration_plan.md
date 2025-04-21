# Migration Plan: Transitioning to Append-Based Historical Data Tracking

This document outlines the step-by-step process for migrating from the current upsert-based approach to an append-based approach with proper historical tracking using dbt snapshots.

## Phase 1: Configuration Changes

1. **Modify Meltano Configuration**
   - Update `meltano.yml` to change the load method from upsert to append:
     ```yaml
     loaders:
       - name: target-postgres
         config:
           load_method: append  # Change from upsert
     ```
   - Ensure `add_record_metadata: true` is set to preserve timestamps

## Phase 2: Staging Layer Implementation

1. **Create Staging Models for Each Entity**
   - Create `models/staging/stg_items.sql`:
     ```sql
     SELECT 
         "Item Name" as item_name,
         "Purchase Cost" as purchase_cost,
         "Sales Price" as sales_price,
         "Quantity On Hand" as quantity_on_hand,
         "Status" as status,
         _sdc_extracted_at,
         _sdc_batched_at
     FROM {{ source('raw', 'items') }}
     ```

   - Create `models/staging/stg_customers.sql`:
     ```sql
     SELECT 
         "QuickBooks Internal Id" as quickbooks_id,
         "Customer" as customer_name,
         -- Other relevant fields
         _sdc_extracted_at,
         _sdc_batched_at
     FROM {{ source('raw', 'customers') }}
     ```

   - Create `models/staging/stg_invoices.sql` and `models/staging/stg_sales_receipts.sql` with similar patterns

2. **Update dbt_project.yml to Include Staging Models**
   - Add staging models configuration:
     ```yaml
     models:
       my_meltano_project:
         staging:
           +materialized: view
     ```

3. **Create sources.yml for Staging Models**
   - Update or create `models/staging/sources.yml` to define the raw data sources

## Phase 3: Snapshot Implementation

1. **Create or Update Snapshot Models**
   - Update `snapshots/items_snapshot.sql`:
     ```sql
     {% snapshot items_snapshot %}
         {{
             config(
               target_schema='snapshots',
               strategy='timestamp',
               unique_key='item_name',
               updated_at='_sdc_batched_at',
             )
         }}
         
         SELECT * FROM {{ ref('stg_items') }}
         
     {% endsnapshot %}
     ```

   - Create similar snapshots for customers, invoices, and sales receipts

2. **Update Pipeline Script**
   - Modify `pipeline.py` to run snapshots after loading data
   - Ensure snapshots are run for each entity type

## Phase 4: Mart Layer Implementation

1. **Update History Tracking Models**
   - Update `models/item_history.sql`:
     ```sql
     WITH changes AS (
         SELECT
             item_name,
             'purchase_cost' as column_name,
             LAG(purchase_cost) OVER (PARTITION BY item_name ORDER BY dbt_valid_from) as old_value,
             purchase_cost as new_value,
             dbt_valid_from as changed_at
         FROM {{ ref('items_snapshot') }}
         
         UNION ALL
         
         -- Similar blocks for sales_price, quantity_on_hand, status
     )

     SELECT * FROM changes
     WHERE old_value IS NULL OR old_value != new_value
     ```

   - Create similar history models for other entities if needed

2. **Update Current State Models**
   - Update `models/products.sql` to use the snapshot for current values:
     ```sql
     SELECT
         item_name,
         sales_description,
         -- Other fields
     FROM {{ ref('items_snapshot') }}
     WHERE dbt_valid_to IS NULL  -- Only current records
     ```

   - Update other current state models similarly

## Phase 5: Testing and Validation

1. **Create Test Cases**
   - Develop test cases to verify historical tracking is working correctly
   - Test with sample data that includes changes over time

2. **Run Full Pipeline Test**
   - Test the full pipeline with a small set of test files
   - Verify that history is being tracked correctly

3. **Validate Results**
   - Query the history tables to ensure changes are being captured
   - Compare results with expected outcomes

## Phase 6: Documentation and Deployment

1. **Update Documentation**
   - Update `docs/item_history_tracking.md` to reflect the new approach
   - Document the new data model and how history is tracked

2. **Deploy to Production**
   - Apply changes to production environment
   - Monitor initial runs to ensure everything works as expected

3. **Create Monitoring Queries**
   - Develop queries to monitor the growth of raw tables
   - Set up regular maintenance tasks if needed

## Phase 7: Cleanup and Optimization

1. **Optimize Storage**
   - Consider partitioning large tables by date
   - Implement archiving strategy for very old data if needed

2. **Performance Tuning**
   - Add indexes to frequently queried columns
   - Optimize slow-running models

## Rollback Plan

In case issues are encountered during migration:

1. **Stop Pipeline**
   - Halt any running pipeline processes

2. **Revert Configuration**
   - Change load_method back to upsert in meltano.yml
   - Revert any other configuration changes

3. **Reimport Data**
   - Rerun the import process with the original configuration
   - All data is available in the original CSV format

4. **Document Issues**
   - Document the issues encountered for future attempts

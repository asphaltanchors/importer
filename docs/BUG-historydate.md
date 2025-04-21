# Item History Date Bug

## Issue Description

In the MQI data pipeline, there's an issue with the `changed_at` field in the `item_history` table. When running the pipeline in 'daily' mode against historical files, the `changed_at` timestamp is always set to the time that the transformation runs, rather than reflecting the actual date of the file being processed.

This means that if we process 10 historical files in a batch, all changes appear to have happened at the same time (the processing time), rather than on their respective dates.

## Current Implementation

### Data Flow

1. Raw data is loaded into `raw.items` table from CSV files
2. `stg_items.sql` creates a staging model from raw data
3. `items_snapshot.sql` creates snapshots using dbt's snapshot functionality
4. `item_history.sql` uses these snapshots to track changes over time

### The Problem

In `item_history.sql`, the `changed_at` field is set to `dbt_valid_from` from the snapshot:

```sql
dbt_valid_from as changed_at
```

When running in daily mode against historical files, `dbt_valid_from` is set to the current transformation run time, not the date of the file.

## Attempted Solutions

We've attempted to modify the system to use the file date instead of the transformation run time:

1. Modified `pipeline.py` to pass the file date as a variable to DBT when running in daily mode:
   ```python
   file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
   result = run_command(f"meltano invoke dbt-postgres:build --vars 'file_date: {file_date}'", dry_run)
   ```

2. Modified `pipeline.py` to also pass the current date as a variable to DBT when running in full mode:
   ```python
   current_date = datetime.now().date()
   result = run_command(f"meltano invoke dbt-postgres:build --vars 'file_date: {current_date}'", dry_run)
   ```

3. Updated `item_history.sql` to use the file date variable instead of `dbt_valid_from` for the `changed_at` field:
   ```sql
   '{{ var("file_date", dbt_valid_from) }}'::date as changed_at
   ```

## Current Status

Despite these changes, the `changed_at` field in the `public.item_history` table is still showing the current date rather than the file date. After dropping the database and running the pipeline again, the issue persists.

## Potential Issues

1. **Variable Passing Issue**: The DBT variable might not be getting passed correctly from the pipeline to the model.

2. **Schema Issue**: The model is being created in the `public` schema rather than a custom schema, which might indicate configuration issues.

3. **Variable Usage in Template**: The Jinja template syntax in the SQL file might not be correctly handling the variable.

## Next Steps for Investigation

1. **Debug Variable Passing**: Add debug statements to the model to verify if the variable is being received:
   ```sql
   -- Debug: {{ var("file_date", "NO DATE PROVIDED") }}
   ```

2. **Try Alternative Variable Syntax**: Use a more explicit approach to setting the date in the model:
   ```sql
   '{{ var("file_date") }}'::date as changed_at
   ```
   Without the fallback to `dbt_valid_from`.

3. **Check Command Format**: Ensure the variable is being passed with the correct format:
   ```python
   result = run_command(f"meltano invoke dbt-postgres:build --vars '{{'file_date': '{file_date}'}}'", dry_run)
   ```

4. **Examine DBT Logs**: Look at the DBT logs to see if there are any errors or warnings related to variable usage.

5. **Check Schema Configuration**: Review the DBT project configuration to ensure models are being created in the expected schemas.

## System Information

- Meltano version: 3.6.0
- DBT version: 1.9.3
- Database: PostgreSQL (localhost:5432, database: mqi, schema: public)

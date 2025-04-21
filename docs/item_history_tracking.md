# Item History Tracking

THIS DOCUIMENT MAY BE OUTDATED. DO NOT TRUST IT. USE FOR INSPIRATION ONLY. VERIFY eVERYTHING!

This document describes the history tracking functionality for items data in the MQI project.

## Overview

The history tracking system captures changes to key attributes of items over time, allowing you to:

- See when values like "Purchase Cost" or "Sales Price" change
- Track the history of changes for any item
- Analyze trends in price changes over time
- Identify items with frequent changes

## Implementation

The history tracking is implemented using the following components:

### 1. `items_snapshot` dbt Snapshot

This snapshot:
- Captures the state of items at each point in time
- Uses dbt's snapshot functionality to track changes
- Stores historical records with valid_from and valid_to timestamps
- Provides the foundation for robust history tracking

File: `transform/snapshots/items_snapshot.sql`

### 2. `item_history` dbt Model

This model:
- Compares current snapshot with previous snapshots
- Records changes to key attributes (purchase_cost, sales_price, quantity_on_hand, status)
- Stores the old value, new value, and when the change was detected
- Uses dbt's snapshot data to accurately track history

File: `transform/models/item_history.sql`

### 3. `item_history_view` dbt Model

This view provides a user-friendly interface to the history data:
- Joins with the products table to get additional item information
- Calculates numeric differences between old and new values
- Calculates percentage changes for price and cost fields
- Orders results with most recent changes first

File: `transform/models/item_history_view.sql`

### 4. Sample Analysis Queries

A set of sample queries demonstrates how to analyze the history data:
- Find items with the largest price increases/decreases
- View history for specific items
- Analyze changes by time period
- Calculate average price changes over time

File: `transform/analysis/item_history_analysis.sql`

## How It Works

1. When the `import_items.sh` script runs, it:
   - Extracts data from the items CSV file
   - Loads it into the raw.items table
   - Creates snapshots to capture the state at this point in time
   - Transforms it into the products table
   - Runs the item_history model to detect and record changes
   - Creates the item_history_view for easy querying

2. The first time the history tracking runs, it will:
   - Create initial records for all items
   - Set old_value to NULL (since there's no previous state)
   - Set new_value to the current value

3. On subsequent runs, it will:
   - Create a new snapshot if there are changes
   - Compare current snapshot with previous snapshots
   - Record only the changes (when a value is different)
   - Store both the old and new values

## Querying History Data

You can query the history data using the `item_history_view` in the analytics schema:

```sql
-- Example: View all changes to Purchase Cost
SELECT 
    item_name,
    sales_description,
    old_value,
    new_value,
    numeric_change,
    percent_change,
    changed_at
FROM analytics.item_history_view
WHERE column_name = 'purchase_cost'
ORDER BY changed_at DESC;
```

See `transform/analysis/item_history_analysis.sql` for more example queries.

## Tracked Columns

The following columns are currently tracked:

- `purchase_cost`: The cost to purchase the item
- `sales_price`: The price at which the item is sold
- `quantity_on_hand`: Current inventory quantity
- `status`: Item status

Additional columns can be added to the tracking by modifying the `item_history.sql` model.

## Benefits of Using Snapshots

The snapshot-based approach provides several advantages:

1. **Robust History Tracking**: dbt snapshots are specifically designed for tracking slowly changing dimensions and maintaining history.

2. **Complete Historical Record**: Every state of each item is preserved, allowing for point-in-time analysis.

3. **Accurate Change Detection**: By comparing snapshots, we can accurately identify what changed and when.

4. **Simplified Maintenance**: dbt handles the complexity of tracking changes, making the system more maintainable.

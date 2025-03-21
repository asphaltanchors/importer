# Item History Tracking

This document describes the history tracking functionality for items data in the MQI project.

## Overview

The history tracking system captures changes to key attributes of items over time, allowing you to:

- See when values like "Purchase Cost" or "Sales Price" change
- Track the history of changes for any item
- Analyze trends in price changes over time
- Identify items with frequent changes

## Implementation

The history tracking is implemented using the following components:

### 1. `item_history` dbt Model

This incremental model:
- Compares current values with previous values
- Records changes to key attributes (purchase_cost, sales_price, quantity_on_hand, status)
- Stores the old value, new value, and when the change was detected
- Uses dbt's incremental materialization to efficiently track changes

File: `transform/models/item_history.sql`

### 2. `item_history_view` dbt Model

This view provides a user-friendly interface to the history data:
- Joins with the products table to get additional item information
- Calculates numeric differences between old and new values
- Calculates percentage changes for price and cost fields
- Orders results with most recent changes first

File: `transform/models/item_history_view.sql`

### 3. Sample Analysis Queries

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
   - Transforms it into the products table
   - Runs the item_history model to detect and record changes
   - Creates the item_history_view for easy querying

2. The first time the history tracking runs, it will:
   - Create initial records for all items
   - Set old_value to NULL (since there's no previous state)
   - Set new_value to the current value

3. On subsequent runs, it will:
   - Compare current values with the most recent values in the history table
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

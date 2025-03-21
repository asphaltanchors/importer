-- Sample queries for analyzing item history data

-- 1. View all changes to Purchase Cost, most recent first
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

-- 2. Find items with the largest price increases (percentage)
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
  AND old_value IS NOT NULL
  AND new_value IS NOT NULL
ORDER BY percent_change DESC
LIMIT 10;

-- 3. Find items with the largest price decreases (percentage)
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
  AND old_value IS NOT NULL
  AND new_value IS NOT NULL
ORDER BY percent_change ASC
LIMIT 10;

-- 4. Count changes by column type
SELECT 
    column_name,
    COUNT(*) as change_count
FROM analytics.item_history_view
GROUP BY column_name
ORDER BY change_count DESC;

-- 5. Find items with the most frequent changes
SELECT 
    item_name,
    COUNT(*) as change_count
FROM analytics.item_history_view
GROUP BY item_name
ORDER BY change_count DESC
LIMIT 10;

-- 6. View history for a specific item
SELECT 
    column_name,
    old_value,
    new_value,
    numeric_change,
    percent_change,
    changed_at
FROM analytics.item_history_view
WHERE item_name = '01-7010.LBR'  -- Replace with your item name
ORDER BY changed_at DESC;

-- 7. Find changes within a specific date range
SELECT 
    item_name,
    column_name,
    old_value,
    new_value,
    numeric_change,
    percent_change,
    changed_at
FROM analytics.item_history_view
WHERE changed_at BETWEEN '2025-01-01' AND '2025-03-31'
ORDER BY changed_at DESC;

-- 8. Calculate average price change over time
SELECT 
    DATE_TRUNC('month', changed_at) as month,
    AVG(CAST(percent_change AS DECIMAL)) as avg_percent_change,
    COUNT(*) as change_count
FROM analytics.item_history_view
WHERE column_name = 'purchase_cost'
  AND percent_change IS NOT NULL
GROUP BY DATE_TRUNC('month', changed_at)
ORDER BY month;

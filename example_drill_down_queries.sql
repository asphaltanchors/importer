-- Example Drill-Down Queries: Company to Customer Analysis
-- These queries demonstrate how to use fct_companies and bridge_customer_company together

-- 1. Top Companies Overview
SELECT 
    company_domain_key,
    company_name,
    customer_count,
    total_revenue,
    revenue_category,
    business_size_category
FROM analytics_mart.fct_companies
ORDER BY total_revenue DESC
LIMIT 10;

-- 2. Drill Down: Who are the customers within Fastenal?
SELECT 
    bc.customer_name,
    bc.customer_total_revenue,
    bc.customer_total_orders,
    bc.customer_value_tier,
    bc.customer_activity_status,
    bc.billing_address_city,
    bc.billing_address_state
FROM analytics_mart.bridge_customer_company bc
WHERE bc.company_domain_key = 'fastenal.com'
  AND bc.customer_total_revenue > 0
ORDER BY bc.customer_total_revenue DESC;

-- 3. Company Analysis with Customer Insights
SELECT 
    fc.company_name,
    fc.total_revenue as company_total_revenue,
    fc.customer_count,
    COUNT(CASE WHEN bc.customer_activity_status = 'Active (Last 90 Days)' THEN 1 END) as active_customers,
    COUNT(CASE WHEN bc.customer_value_tier LIKE 'High Value%' THEN 1 END) as high_value_customers,
    COUNT(CASE WHEN bc.is_individual_customer THEN 1 END) as individual_customers,
    MAX(bc.customer_total_revenue) as top_customer_revenue,
    AVG(bc.customer_total_revenue) as avg_customer_revenue
FROM analytics_mart.fct_companies fc
INNER JOIN analytics_mart.bridge_customer_company bc 
    ON fc.company_domain_key = bc.company_domain_key
WHERE fc.total_revenue > 50000  -- High value companies only
GROUP BY fc.company_domain_key, fc.company_name, fc.total_revenue, fc.customer_count
ORDER BY fc.total_revenue DESC;

-- 4. Customer Activity Analysis by Company Type
SELECT 
    CASE 
        WHEN fc.customer_count = 1 THEN 'Single Location'
        WHEN fc.customer_count BETWEEN 2 AND 10 THEN 'Small Multi-Location'
        ELSE 'Large Multi-Location'
    END as company_size,
    COUNT(DISTINCT fc.company_domain_key) as company_count,
    COUNT(bc.customer_id) as total_customers,
    COUNT(CASE WHEN bc.customer_activity_status = 'Active (Last 90 Days)' THEN 1 END) as active_customers,
    ROUND(
        COUNT(CASE WHEN bc.customer_activity_status = 'Active (Last 90 Days)' THEN 1 END) * 100.0 / 
        COUNT(bc.customer_id), 2
    ) as active_customer_percentage,
    SUM(bc.customer_total_revenue) as total_revenue
FROM analytics_mart.fct_companies fc
INNER JOIN analytics_mart.bridge_customer_company bc 
    ON fc.company_domain_key = bc.company_domain_key
WHERE fc.domain_type = 'corporate'
GROUP BY 
    CASE 
        WHEN fc.customer_count = 1 THEN 'Single Location'
        WHEN fc.customer_count BETWEEN 2 AND 10 THEN 'Small Multi-Location'
        ELSE 'Large Multi-Location'
    END
ORDER BY total_revenue DESC;

-- 5. Find the Best Customers within Top Companies
-- This query finds the highest-value individual customer within each top company
WITH top_companies AS (
    SELECT company_domain_key, company_name, total_revenue
    FROM analytics_mart.fct_companies
    WHERE total_revenue > 25000
),
best_customer_per_company AS (
    SELECT 
        tc.company_name,
        tc.total_revenue as company_revenue,
        bc.customer_name as best_customer,
        bc.customer_total_revenue as best_customer_revenue,
        bc.customer_total_orders as best_customer_orders,
        bc.customer_activity_status,
        ROW_NUMBER() OVER (PARTITION BY tc.company_domain_key ORDER BY bc.customer_total_revenue DESC) as rn
    FROM top_companies tc
    INNER JOIN analytics_mart.bridge_customer_company bc 
        ON tc.company_domain_key = bc.company_domain_key
    WHERE bc.customer_total_revenue > 0
)
SELECT 
    company_name,
    company_revenue,
    best_customer,
    best_customer_revenue,
    best_customer_orders,
    customer_activity_status,
    ROUND(best_customer_revenue * 100.0 / company_revenue, 1) as customer_revenue_percentage
FROM best_customer_per_company
WHERE rn = 1
ORDER BY company_revenue DESC;
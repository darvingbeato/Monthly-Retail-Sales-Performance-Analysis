USE WideWorldImporters;

/* =========================================================
   1. DATA QUALITY AUDIT (ALWAYS FIRST)
   ========================================================= */

-- Null check across all columns
SELECT
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) AS Null_Order_ID,
    SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) AS Null_Order_Date,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS Null_Customer_ID,
    SUM(CASE WHEN store_location IS NULL THEN 1 ELSE 0 END) AS Null_Store_Location,
    SUM(CASE WHEN product_category IS NULL THEN 1 ELSE 0 END) AS Null_Product_Category,
    SUM(CASE WHEN quantity IS NULL THEN 1 ELSE 0 END) AS Null_Quantity,
    SUM(CASE WHEN unit_price IS NULL THEN 1 ELSE 0 END) AS Null_Unit_Price,
    SUM(CASE WHEN discount IS NULL THEN 1 ELSE 0 END) AS Null_Discount,
    SUM(CASE WHEN revenue IS NULL THEN 1 ELSE 0 END) AS Null_Revenue,
    SUM(CASE WHEN sales_channel IS NULL THEN 1 ELSE 0 END) AS Null_Sales_Channel,
    SUM(CASE WHEN payment_method IS NULL THEN 1 ELSE 0 END) AS Null_Payment_Method,
    SUM(CASE WHEN customer_type IS NULL THEN 1 ELSE 0 END) AS Null_Customer_Type
FROM retail_sales;


-- Data sanity checks
SELECT *
FROM retail_sales
WHERE quantity < 0
   OR unit_price < 0
   OR revenue < 0
   OR discount NOT BETWEEN 0 AND 1;


-- Duplicate check (true duplicates)
SELECT order_id, COUNT(*) AS duplicate_count
FROM retail_sales
GROUP BY order_id
HAVING COUNT(*) > 1;



/* =========================================================
   2. DATA CLEANING & TRANSFORMATION
   ========================================================= */

DROP TABLE IF EXISTS #CleanRetailSales;

WITH Cleaned AS (
    SELECT
        order_id,
        CAST(order_date AS DATE) AS order_date,
        customer_id,
        UPPER(TRIM(store_location)) AS store_location,
        UPPER(TRIM(product_category)) AS product_category,      
        ROUND(
            CASE WHEN quantity IS NULL AND revenue IS NOT NULL AND unit_price > 0
                 THEN revenue / (unit_price * (1 - ISNULL(discount,0)))
                 ELSE quantity END, 0) AS quantity,-- Fix quantity using business logic. Formula Quantity= Revenue / (Unit Price×(1−Discount))

        unit_price,
        ISNULL(discount, 0) AS discount,
        ROUND(
            CASE WHEN revenue IS NULL AND quantity IS NOT NULL
                 THEN (unit_price * (1 - ISNULL(discount,0))) * quantity
                 ELSE revenue END, 2) AS revenue,-- Fix revenue using business logic. Formula Revenue=Unit Price×(1−Discount)×Quantity

        sales_channel,
        payment_method,
        customer_type

    FROM retail_sales
    WHERE quantity IS NOT NULL OR revenue IS NOT NULL
),

Deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY order_date DESC
        ) AS rn
    FROM Cleaned
)

SELECT *
INTO #CleanRetailSales
FROM Deduplicated
WHERE rn = 1;



/* =========================================================
   3. POST-CLEAN VALIDATION
   ========================================================= */

-- Check remaining nulls
SELECT *
FROM #CleanRetailSales
WHERE quantity IS NULL OR revenue IS NULL;

-- Row count comparison
SELECT 
    (SELECT COUNT(*) FROM retail_sales) AS Original_Count,
    (SELECT COUNT(*) FROM #CleanRetailSales) AS Cleaned_Count;



/* =========================================================
   4. CORE KPIs (EXECUTIVE LEVEL)
   ========================================================= */

SELECT
    ROUND(SUM(revenue), 2) AS TotalRevenue,
    COUNT(DISTINCT order_id) AS TotalOrders,
    COUNT(DISTINCT customer_id) AS TotalCustomers,
    ROUND(CAST(SUM(quantity) AS DECIMAL(10,2)) / COUNT(DISTINCT order_id), 2) AS AVG_units_per_order,
    ROUND(SUM(revenue) / NULLIF(COUNT(DISTINCT order_id), 0), 2) AS AvgOrderValue
FROM #CleanRetailSales;




/* =========================================================
   5. MONTHLY PERFORMANCE + GROWTH
   ========================================================= */
   WITH Monthly AS (
    SELECT
        DATEFROMPARTS(YEAR(order_date), MONTH(order_date), 1) AS month_start,
        SUM(revenue) AS revenue,
        COUNT(DISTINCT order_id) AS orders,
        COUNT(DISTINCT customer_id) AS customers,
     
        ROUND(SUM(revenue) / NULLIF(COUNT(DISTINCT order_id), 0), 2) AS AOV
    FROM #CleanRetailSales
    GROUP BY DATEFROMPARTS(YEAR(order_date), MONTH(order_date), 1)
),
Growth AS (
    SELECT *,
        LAG(revenue) OVER (ORDER BY month_start) AS prev_revenue,
        LAG(customers) OVER (ORDER BY month_start) AS prev_customers,
        LAG(orders) OVER (ORDER BY month_start) AS prev_orders,
        LAG(AOV) OVER (ORDER BY month_start) AS prev_AOV
    FROM Monthly
)
SELECT
    month_start,
    revenue,
    prev_revenue,
    
    ROUND((revenue - prev_revenue) / NULLIF(prev_revenue,0), 4) AS revenue_growth_pct,
    orders,
    prev_orders,
    ROUND((cast(orders as decimal(10,2)) - prev_orders) / NULLIF(prev_orders,0), 4) AS orders_growth_pct,

    Customers,
    prev_Customers,
    ROUND((cast(Customers as decimal(10,2)) - prev_Customers) / NULLIF(prev_Customers,0), 4) AS Customers_growth_pct,


    AOV,
    prev_AOV,
    ROUND((AOV - prev_AOV) / NULLIF(prev_AOV,0), 4) AS AOV_growth_pct
FROM Growth
ORDER BY month_start;

/* =========================================================
   6. BUSINESS ANALYSIS (INSIGHT-DRIVEN)
   ========================================================= */

-- Store Performance + Efficiency
SELECT
    store_location,
    SUM(revenue) AS revenue,
    COUNT(DISTINCT order_id) AS orders,
    ROUND(SUM(revenue) / COUNT(DISTINCT order_id), 2) AS revenue_per_order
FROM #CleanRetailSales
GROUP BY store_location
ORDER BY revenue DESC;


-- Product Category Performance
SELECT
    product_category,
    SUM(revenue) AS revenue,
    ROUND(AVG(revenue),2) AS avg_transaction_value
FROM #CleanRetailSales
GROUP BY product_category
ORDER BY revenue DESC;


-- Customer Value Analysis
SELECT
    customer_type,
    COUNT(DISTINCT customer_id) AS customers,
    SUM(revenue) AS revenue,
    ROUND(SUM(revenue) / COUNT(DISTINCT customer_id), 2) AS revenue_per_customer
FROM #CleanRetailSales
GROUP BY customer_type;


-- Channel Effectiveness
SELECT
    sales_channel,
    SUM(revenue) AS revenue,
    COUNT(DISTINCT order_id) AS orders,
    ROUND(SUM(revenue)/COUNT(DISTINCT order_id),2) AS avg_order_value
FROM #CleanRetailSales
GROUP BY sales_channel
ORDER BY revenue DESC;



/* =========================================================
   7. ADVANCED DISCOUNT ANALYSIS (REAL INSIGHT)
   ========================================================= */

SELECT
    CASE 
        WHEN discount = 0 THEN 'No Discount'
        WHEN discount > 0 AND discount <= 0.1 THEN 'Low Discount'
        WHEN discount > 0.1 AND discount <= 0.3 THEN 'Medium Discount'
        ELSE 'High Discount'
    END AS discount_bucket,

    COUNT(DISTINCT order_id) AS orders,
    SUM(revenue) AS revenue,
    ROUND(SUM(revenue)/COUNT(DISTINCT order_id),2) AS avg_order_value

FROM #CleanRetailSales
GROUP BY 
    CASE 
        WHEN discount = 0 THEN 'No Discount'
        WHEN discount > 0 AND discount <= 0.1 THEN 'Low Discount'
        WHEN discount > 0.1 AND discount <= 0.3 THEN 'Medium Discount'
        ELSE 'High Discount'
    END
ORDER BY revenue DESC;



/* =========================================================
   8. HIGH-VALUE CUSTOMER IDENTIFICATION
   ========================================================= */

SELECT TOP 10
    customer_id,
    SUM(revenue) AS total_spent,
    COUNT(DISTINCT order_id) AS orders,
    ROUND(SUM(revenue)/COUNT(DISTINCT order_id),2) AS avg_order_value
FROM #CleanRetailSales
GROUP BY customer_id
ORDER BY total_spent DESC;

-- Q1 Monthly revenue and items sold (joins + aggregation)
SELECT d.cal_year, d.cal_month,
       COUNT(DISTINCT s.order_id) AS orders,
       SUM(s.price) AS revenue
FROM olist.fact_sales s
JOIN olist.dim_date d ON s.purchase_date_key = d.date_key
GROUP BY d.cal_year, d.cal_month
ORDER BY d.cal_year, d.cal_month;

-- Q2 Top 10 product categories by revenue
SELECT p.category_en, ROUND(SUM(s.price), 2) AS revenue
FROM olist.fact_sales s
JOIN olist.dim_product p ON s.product_key = p.product_key
GROUP BY p.category_en
ORDER BY revenue DESC
LIMIT 10;

-- Q3 Revenue by customer region (uses the API-sourced geography)
SELECT g.region_name, g.state_name, ROUND(SUM(s.price), 2) AS revenue
FROM olist.fact_sales s
JOIN olist.dim_customer c ON s.customer_key = c.customer_key
JOIN olist.dim_geography g ON c.geography_key = g.geography_key
GROUP BY g.region_name, g.state_name
ORDER BY revenue DESC;

-- Q4 Late-delivery rate by state (delivered orders only)
SELECT g.state_name,
       COUNT(*) AS delivered_orders,
       ROUND(100.0 * SUM(o.is_late) / COUNT(*), 2) AS late_pct
FROM olist.fact_orders o
JOIN olist.dim_customer c ON o.customer_key = c.customer_key
JOIN olist.dim_geography g ON c.geography_key = g.geography_key
WHERE o.is_late IS NOT NULL
GROUP BY g.state_name
ORDER BY late_pct DESC;

-- Q5 Payment method share (window over an aggregate)
SELECT pt.payment_type,
       COUNT(*) AS payments,
       ROUND(SUM(p.payment_value), 2) AS total_value,
       ROUND(100.0 * SUM(p.payment_value) / SUM(SUM(p.payment_value)) OVER (), 1) AS pct
FROM olist.fact_payments p
JOIN olist.dim_payment_type pt ON p.payment_type_key = pt.payment_type_key
GROUP BY pt.payment_type
ORDER BY total_value DESC;

-- Q6 Does lateness hurt reviews?
SELECT is_late, COUNT(*) AS orders, ROUND(AVG(avg_review), 2) AS avg_review_score
FROM olist.fact_orders
WHERE is_late IS NOT NULL AND avg_review IS NOT NULL
GROUP BY is_late;

-- Q7 Top 3 sellers by revenue inside each state (CTE + RANK)
WITH seller_rev AS (
    SELECT sl.seller_state, sl.seller_id, SUM(s.price) AS revenue
    FROM olist.fact_sales s
    JOIN olist.dim_seller sl ON s.seller_key = sl.seller_key
    GROUP BY sl.seller_state, sl.seller_id
)
SELECT seller_state, seller_id, revenue, rnk
FROM (
    SELECT seller_state, seller_id, revenue,
           RANK() OVER (PARTITION BY seller_state ORDER BY revenue DESC) AS rnk
    FROM seller_rev
) t
WHERE rnk <= 3
ORDER BY seller_state, rnk;

-- Q8 Month-over-month revenue growth (CTE + LAG)
WITH m AS (
    SELECT d.cal_year, d.cal_month, SUM(s.price) AS revenue
    FROM olist.fact_sales s
    JOIN olist.dim_date d ON s.purchase_date_key = d.date_key
    GROUP BY d.cal_year, d.cal_month
)
SELECT cal_year, cal_month, revenue,
       LAG(revenue) OVER (ORDER BY cal_year, cal_month) AS prev_revenue,
       ROUND(100.0 * (revenue - LAG(revenue) OVER (ORDER BY cal_year, cal_month))
             / NULLIF(LAG(revenue) OVER (ORDER BY cal_year, cal_month), 0), 1) AS growth_pct
FROM m
ORDER BY cal_year, cal_month;

-- sales total in fact_sales equals the items total in fact_orders
SELECT (SELECT SUM(price) FROM olist.fact_sales)        AS sales_total,
       (SELECT SUM(items_value) FROM olist.fact_orders) AS orders_total;

-- orphan foreign keys (all should return 0)
SELECT COUNT(*) FROM olist.fact_sales s
LEFT JOIN olist.dim_product p ON s.product_key = p.product_key
WHERE p.product_key IS NULL;

SELECT COUNT(*) FROM olist.fact_sales s
LEFT JOIN olist.dim_seller sl ON s.seller_key = sl.seller_key
WHERE sl.seller_key IS NULL;

SELECT COUNT(*) FROM olist.fact_sales s
LEFT JOIN olist.dim_date d ON s.delivered_date_key = d.date_key
WHERE d.date_key IS NULL;

-- duplicate natural keys in a dimension (should return 0 rows)
SELECT customer_id, COUNT(*) FROM olist.dim_customer
GROUP BY customer_id
HAVING COUNT(*) > 1;

--Performance work
SELECT d.cal_year, d.cal_month,
       COUNT(DISTINCT s.order_id) AS orders,
       SUM(s.price) AS revenue
FROM olist.fact_sales s
JOIN olist.dim_date d ON s.purchase_date_key = d.date_key
GROUP BY d.cal_year, d.cal_month
ORDER BY d.cal_year, d.cal_month;

--Materialized view.

CREATE MATERIALIZED VIEW olist.mv_monthly_sales AS
SELECT d.cal_year, d.cal_month, COUNT(*) AS items_sold, SUM(s.price) AS revenue
FROM olist.fact_sales s
JOIN olist.dim_date d ON s.purchase_date_key = d.date_key
GROUP BY d.cal_year, d.cal_month; 
-- =====================================================================
-- 15 Business-scenario analytics queries (from the project spec).
-- All run against the Gold star schema (ecommerce.gold).
-- Each query answers one numbered business scenario.
-- =====================================================================

-- 1. Daily revenue trend (detect drops/spikes)
SELECT d.date, ROUND(SUM(f.payment_value), 2) AS daily_revenue, COUNT(DISTINCT f.order_id) AS orders
FROM ecommerce.gold.fact_orders f
JOIN ecommerce.gold.dim_date d ON f.date_id = d.date_id
WHERE f.order_status = 'delivered'
GROUP BY d.date
ORDER BY d.date;

-- 2. Month-over-month revenue growth (finance/business health)
WITH monthly AS (
  SELECT d.year_month, SUM(f.payment_value) AS revenue
  FROM ecommerce.gold.fact_orders f
  JOIN ecommerce.gold.dim_date d ON f.date_id = d.date_id
  GROUP BY d.year_month
)
SELECT year_month,
       ROUND(revenue, 2) AS revenue,
       ROUND(LAG(revenue) OVER (ORDER BY year_month), 2) AS prev_month,
       ROUND(100 * (revenue - LAG(revenue) OVER (ORDER BY year_month))
             / NULLIF(LAG(revenue) OVER (ORDER BY year_month), 0), 2) AS mom_growth_pct
FROM monthly
ORDER BY year_month;

-- 3. Top 10 sellers by revenue (reward top sellers)
SELECT f.seller_id, s.seller_city, s.seller_state,
       ROUND(SUM(f.payment_value), 2) AS revenue, COUNT(*) AS items_sold
FROM ecommerce.gold.fact_orders f
JOIN ecommerce.gold.dim_seller s ON f.seller_sk = s.seller_sk
GROUP BY f.seller_id, s.seller_city, s.seller_state
ORDER BY revenue DESC
LIMIT 10;

-- 4. Seller performance over time using SCD-2 location history
--    Revenue + avg delivery days split by each historical seller location version.
SELECT s.seller_id, s.seller_city, s.seller_state,
       s.effective_from, s.effective_to, s.is_current,
       ROUND(SUM(f.payment_value), 2) AS revenue,
       ROUND(AVG(f.delivery_days), 1) AS avg_delivery_days
FROM ecommerce.gold.fact_orders f
JOIN ecommerce.gold.dim_seller s ON f.seller_sk = s.seller_sk
GROUP BY s.seller_id, s.seller_city, s.seller_state, s.effective_from, s.effective_to, s.is_current
ORDER BY s.seller_id, s.effective_from;

-- 5. Category revenue analysis (which categories drive revenue)
SELECT p.product_category_name_english AS category,
       ROUND(SUM(f.payment_value), 2) AS revenue, COUNT(*) AS items_sold
FROM ecommerce.gold.fact_orders f
JOIN ecommerce.gold.dim_product p ON f.product_sk = p.product_sk
GROUP BY p.product_category_name_english
ORDER BY revenue DESC;

-- 6. Low-performing categories (discontinue/promote)
SELECT p.product_category_name_english AS category,
       ROUND(SUM(f.payment_value), 2) AS revenue, COUNT(*) AS items_sold,
       ROUND(AVG(f.review_score), 2) AS avg_review
FROM ecommerce.gold.fact_orders f
JOIN ecommerce.gold.dim_product p ON f.product_sk = p.product_sk
GROUP BY p.product_category_name_english
HAVING COUNT(*) > 0
ORDER BY revenue ASC
LIMIT 15;

-- 7. Sellers causing late deliveries (actual > estimated)
SELECT f.seller_id,
       COUNT(*) AS delivered_items,
       ROUND(AVG(f.delivery_days), 1) AS avg_delivery_days,
       SUM(CASE WHEN f.delivery_days > 15 THEN 1 ELSE 0 END) AS late_items,
       ROUND(100.0 * SUM(CASE WHEN f.delivery_days > 15 THEN 1 ELSE 0 END) / COUNT(*), 1) AS late_pct
FROM ecommerce.gold.fact_orders f
WHERE f.delivery_days IS NOT NULL
GROUP BY f.seller_id
HAVING COUNT(*) >= 20
ORDER BY late_pct DESC
LIMIT 20;

-- 8. Do late deliveries reduce ratings? (delivery impact on reviews)
SELECT CASE WHEN f.delivery_days <= 7 THEN '0-7 days'
            WHEN f.delivery_days <= 15 THEN '8-15 days'
            WHEN f.delivery_days <= 30 THEN '16-30 days'
            ELSE '30+ days' END AS delivery_bucket,
       COUNT(*) AS items,
       ROUND(AVG(f.review_score), 3) AS avg_review_score
FROM ecommerce.gold.fact_orders f
WHERE f.delivery_days IS NOT NULL AND f.review_score > 0
GROUP BY 1
ORDER BY MIN(f.delivery_days);

-- 9. City/state-wise revenue (regional managers)
SELECT c.customer_state, c.customer_city,
       ROUND(SUM(f.payment_value), 2) AS revenue, COUNT(DISTINCT f.order_id) AS orders
FROM ecommerce.gold.fact_orders f
JOIN ecommerce.gold.dim_customer c ON f.customer_sk = c.customer_sk
GROUP BY c.customer_state, c.customer_city
ORDER BY revenue DESC
LIMIT 25;

-- 10. Customer repeat rate (retention)
WITH per_customer AS (
  SELECT c.customer_unique_id, COUNT(DISTINCT f.order_id) AS order_count
  FROM ecommerce.gold.fact_orders f
  JOIN ecommerce.gold.dim_customer c ON f.customer_sk = c.customer_sk
  GROUP BY c.customer_unique_id
)
SELECT COUNT(*) AS total_customers,
       SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END) AS repeat_customers,
       ROUND(100.0 * SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS repeat_rate_pct
FROM per_customer;

-- 11. Installments & payment patterns (finance)
SELECT f.payment_type, f.payment_installments,
       COUNT(*) AS payments, ROUND(SUM(f.payment_value), 2) AS total_value,
       ROUND(AVG(f.payment_value), 2) AS avg_value
FROM ecommerce.gold.fact_orders f
WHERE f.payment_type IS NOT NULL
GROUP BY f.payment_type, f.payment_installments
ORDER BY f.payment_type, f.payment_installments;

-- 12. Revenue per customer (target premium customers)
SELECT c.customer_unique_id,
       ROUND(SUM(f.payment_value), 2) AS lifetime_value,
       COUNT(DISTINCT f.order_id) AS orders
FROM ecommerce.gold.fact_orders f
JOIN ecommerce.gold.dim_customer c ON f.customer_sk = c.customer_sk
GROUP BY c.customer_unique_id
ORDER BY lifetime_value DESC
LIMIT 50;

-- 13. Seller risk identification (low rating + late + low volume)
SELECT f.seller_id,
       COUNT(*) AS items,
       ROUND(AVG(f.review_score), 2) AS avg_review,
       ROUND(AVG(f.delivery_days), 1) AS avg_delivery_days,
       ROUND(SUM(f.payment_value), 2) AS revenue
FROM ecommerce.gold.fact_orders f
WHERE f.review_score > 0
GROUP BY f.seller_id
HAVING AVG(f.review_score) < 3 AND AVG(f.delivery_days) > 15
ORDER BY avg_review ASC, avg_delivery_days DESC
LIMIT 25;

-- 14. Seasonal sales analysis (plan festival promotions)
SELECT d.year, d.month, d.month_name,
       ROUND(SUM(f.payment_value), 2) AS revenue, COUNT(DISTINCT f.order_id) AS orders
FROM ecommerce.gold.fact_orders f
JOIN ecommerce.gold.dim_date d ON f.date_id = d.date_id
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

-- 15. Product performance (promote or discontinue)
SELECT f.product_id, p.product_category_name_english AS category,
       COUNT(*) AS items_sold,
       ROUND(SUM(f.payment_value), 2) AS revenue,
       ROUND(AVG(f.review_score), 2) AS avg_review
FROM ecommerce.gold.fact_orders f
JOIN ecommerce.gold.dim_product p ON f.product_sk = p.product_sk
GROUP BY f.product_id, p.product_category_name_english
ORDER BY revenue DESC
LIMIT 50;

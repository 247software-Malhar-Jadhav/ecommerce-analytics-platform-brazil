-- =====================================================================
-- 15 Business-scenario analytics queries (from the project spec).
-- All run against the Gold star schema (ecommerce.gold).
-- Each query answers one numbered business scenario.
-- =====================================================================

-- 1. Daily revenue trend (detect drops/spikes)
-- Business question: How much revenue and how many orders do we get each day?
-- How it works: join the fact to dim_date to get a real calendar date, keep only
-- delivered orders, then SUM revenue and COUNT DISTINCT orders per day.
SELECT d.date, ROUND(SUM(f.payment_value), 2) AS daily_revenue, COUNT(DISTINCT f.order_id) AS orders
FROM ecommerce.gold.fact_orders f
JOIN ecommerce.gold.dim_date d ON f.date_id = d.date_id
WHERE f.order_status = 'delivered'
GROUP BY d.date
ORDER BY d.date;

-- 2. Month-over-month revenue growth (finance/business health)
-- Business question: Is monthly revenue growing or shrinking vs the previous month?
-- How it works: a CTE first aggregates revenue per year_month. Then the window
-- function LAG() looks at the previous month's revenue (ordered by year_month) so we
-- can compute % growth. NULLIF(...,0) avoids divide-by-zero on the first month.
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
-- Business question: Which 10 sellers bring in the most money?
-- How it works: join fact to dim_seller (on the surrogate key seller_sk), SUM
-- revenue per seller, ORDER BY revenue DESC and keep only the top 10.
SELECT f.seller_id, s.seller_city, s.seller_state,
       ROUND(SUM(f.payment_value), 2) AS revenue, COUNT(*) AS items_sold
FROM ecommerce.gold.fact_orders f
JOIN ecommerce.gold.dim_seller s ON f.seller_sk = s.seller_sk
GROUP BY f.seller_id, s.seller_city, s.seller_state
ORDER BY revenue DESC
LIMIT 10;

-- 4. Seller performance over time using SCD-2 location history
--    Revenue + avg delivery days split by each historical seller location version.
-- How it works: because dim_seller is SCD-2, each location change is a separate row
-- (with effective_from/to). Joining on seller_sk and grouping by those history columns
-- attributes each order to the seller location that was current when it happened.
SELECT s.seller_id, s.seller_city, s.seller_state,
       s.effective_from, s.effective_to, s.is_current,
       ROUND(SUM(f.payment_value), 2) AS revenue,
       ROUND(AVG(f.delivery_days), 1) AS avg_delivery_days
FROM ecommerce.gold.fact_orders f
JOIN ecommerce.gold.dim_seller s ON f.seller_sk = s.seller_sk
GROUP BY s.seller_id, s.seller_city, s.seller_state, s.effective_from, s.effective_to, s.is_current
ORDER BY s.seller_id, s.effective_from;

-- 5. Category revenue analysis (which categories drive revenue)
-- Business question: Which product categories generate the most revenue?
-- How it works: join fact to dim_product to reach the English category name, then
-- SUM revenue and COUNT items per category, sorted highest revenue first.
SELECT p.product_category_name_english AS category,
       ROUND(SUM(f.payment_value), 2) AS revenue, COUNT(*) AS items_sold
FROM ecommerce.gold.fact_orders f
JOIN ecommerce.gold.dim_product p ON f.product_sk = p.product_sk
GROUP BY p.product_category_name_english
ORDER BY revenue DESC;

-- 6. Low-performing categories (discontinue/promote)
-- Business question: Which categories earn the least (and how are they rated)?
-- How it works: same per-category aggregation as #5 but also averages review_score,
-- then ORDER BY revenue ASC (lowest first) and keeps the bottom 15 categories.
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
-- Business question: Which sellers most often deliver late (>15 days)?
-- How it works: per seller, count delivered items and use SUM(CASE WHEN ...) to count
-- and percentage the late ones. HAVING COUNT(*) >= 20 ignores tiny sellers so the
-- late_pct is statistically meaningful; sorted by worst late_pct.
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
-- Business question: Do slower deliveries lead to lower review scores?
-- How it works: a CASE expression buckets each item by delivery speed, then we
-- average the review_score within each bucket. GROUP BY 1 groups by the first SELECT
-- column (the bucket); ORDER BY MIN(delivery_days) keeps buckets in time order.
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
-- Business question: Which customer cities/states generate the most revenue?
-- How it works: join fact to dim_customer for location, SUM revenue and COUNT DISTINCT
-- orders grouped by state + city, top 25 regions by revenue.
SELECT c.customer_state, c.customer_city,
       ROUND(SUM(f.payment_value), 2) AS revenue, COUNT(DISTINCT f.order_id) AS orders
FROM ecommerce.gold.fact_orders f
JOIN ecommerce.gold.dim_customer c ON f.customer_sk = c.customer_sk
GROUP BY c.customer_state, c.customer_city
ORDER BY revenue DESC
LIMIT 25;

-- 10. Customer repeat rate (retention)
-- Business question: What share of customers order more than once?
-- How it works: the CTE counts orders per unique customer (customer_unique_id tracks a
-- person across customer_id values). The outer query then counts how many had >1 order
-- and divides by total customers to get the repeat-rate percentage.
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
-- Business question: How do customers pay (type) and in how many installments?
-- How it works: group by payment_type and payment_installments, then count payments
-- and total/average value to reveal the most common payment behaviors.
SELECT f.payment_type, f.payment_installments,
       COUNT(*) AS payments, ROUND(SUM(f.payment_value), 2) AS total_value,
       ROUND(AVG(f.payment_value), 2) AS avg_value
FROM ecommerce.gold.fact_orders f
WHERE f.payment_type IS NOT NULL
GROUP BY f.payment_type, f.payment_installments
ORDER BY f.payment_type, f.payment_installments;

-- 12. Revenue per customer (target premium customers)
-- Business question: Who are our highest lifetime-value customers?
-- How it works: SUM all payments per customer_unique_id (their lifetime value),
-- count their orders, and return the top 50 spenders.
SELECT c.customer_unique_id,
       ROUND(SUM(f.payment_value), 2) AS lifetime_value,
       COUNT(DISTINCT f.order_id) AS orders
FROM ecommerce.gold.fact_orders f
JOIN ecommerce.gold.dim_customer c ON f.customer_sk = c.customer_sk
GROUP BY c.customer_unique_id
ORDER BY lifetime_value DESC
LIMIT 50;

-- 13. Seller risk identification (low rating + late + low volume)
-- Business question: Which sellers are risky (poor ratings AND slow delivery)?
-- How it works: aggregate per seller, then HAVING filters to sellers whose AVG review
-- is below 3 AND AVG delivery is over 15 days. Worst reviews/slowest delivery first.
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
-- Business question: How does revenue vary by month/season across years?
-- How it works: join to dim_date and group by year + month to spot seasonal peaks
-- (e.g. holidays) that inform promotion planning.
SELECT d.year, d.month, d.month_name,
       ROUND(SUM(f.payment_value), 2) AS revenue, COUNT(DISTINCT f.order_id) AS orders
FROM ecommerce.gold.fact_orders f
JOIN ecommerce.gold.dim_date d ON f.date_id = d.date_id
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

-- 15. Product performance (promote or discontinue)
-- Business question: Which individual products sell and review best/worst?
-- How it works: join fact to dim_product, group by product, and compute items sold,
-- revenue and average review per product; top 50 products by revenue.
SELECT f.product_id, p.product_category_name_english AS category,
       COUNT(*) AS items_sold,
       ROUND(SUM(f.payment_value), 2) AS revenue,
       ROUND(AVG(f.review_score), 2) AS avg_review
FROM ecommerce.gold.fact_orders f
JOIN ecommerce.gold.dim_product p ON f.product_sk = p.product_sk
GROUP BY f.product_id, p.product_category_name_english
ORDER BY revenue DESC
LIMIT 50;

-- =====================================================================
-- Delta Lake TIME TRAVEL queries (spec requirement).
-- Demonstrates version/timestamp reads, history, diff and restore.
-- =====================================================================

-- 1. Show full version history of the fact table.
DESCRIBE HISTORY ecommerce.gold.fact_orders;

-- 2. Read a specific VERSION.
SELECT COUNT(*) AS rows_at_v2
FROM ecommerce.gold.fact_orders VERSION AS OF 2;

-- 3. Read AS OF a timestamp (point-in-time reporting).
SELECT ROUND(SUM(payment_value), 2) AS revenue_as_of_yesterday
FROM ecommerce.gold.fact_orders TIMESTAMP AS OF date_sub(current_date(), 1);

-- 4. Diff two versions — how many rows did the latest load add?
SELECT
  (SELECT COUNT(*) FROM ecommerce.gold.fact_orders) AS current_rows,
  (SELECT COUNT(*) FROM ecommerce.gold.fact_orders VERSION AS OF 0) AS first_load_rows;

-- 5. Audit SCD-2 seller dimension history (no time travel needed — history is in the table).
SELECT seller_id, seller_city, seller_state, effective_from, effective_to, is_current
FROM ecommerce.gold.dim_seller
WHERE is_current = false
ORDER BY seller_id, effective_from;

-- 6. Compare a Silver table before/after the most recent overwrite.
SELECT 'before' AS v, COUNT(*) AS rows FROM ecommerce.silver.orders VERSION AS OF 0
UNION ALL
SELECT 'after'  AS v, COUNT(*) AS rows FROM ecommerce.silver.orders;

-- 7. RESTORE a table to a previous good version (DESTRUCTIVE — uncomment to use).
-- RESTORE TABLE ecommerce.gold.fact_orders TO VERSION AS OF 3;

-- 8. Control the time-travel retention window.
-- SET spark.databricks.delta.retentionDurationCheck.enabled = false;  -- only if shortening below 7 days
-- VACUUM ecommerce.gold.fact_orders RETAIN 168 HOURS;   -- keep 7 days of history

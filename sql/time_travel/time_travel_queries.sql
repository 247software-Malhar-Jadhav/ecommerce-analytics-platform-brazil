-- =====================================================================
-- Delta Lake TIME TRAVEL queries (spec requirement).
-- Demonstrates version/timestamp reads, history, diff and restore.
-- =====================================================================

-- 1. Show full version history of the fact table.
-- DESCRIBE HISTORY returns Delta's commit log (version, timestamp, operation). This is
-- the audit trail that every time-travel query below relies on.
DESCRIBE HISTORY ecommerce.gold.fact_orders;

-- 2. Read a specific VERSION.
-- "VERSION AS OF 2" reads the table exactly as it looked at commit #2, regardless of
-- later changes. Useful to inspect an old snapshot by its version number.
SELECT COUNT(*) AS rows_at_v2
FROM ecommerce.gold.fact_orders VERSION AS OF 2;

-- 3. Read AS OF a timestamp (point-in-time reporting).
-- "TIMESTAMP AS OF <ts>" reads the table as of a moment in time (here, yesterday).
-- Great for reproducing "revenue as reported yesterday" even after new data lands.
SELECT ROUND(SUM(payment_value), 2) AS revenue_as_of_yesterday
FROM ecommerce.gold.fact_orders TIMESTAMP AS OF date_sub(current_date(), 1);

-- 4. Diff two versions — how many rows did the latest load add?
-- Two correlated subqueries count rows now vs at the very first commit (version 0);
-- subtracting them shows how much the table has grown since it was created.
SELECT
  (SELECT COUNT(*) FROM ecommerce.gold.fact_orders) AS current_rows,
  (SELECT COUNT(*) FROM ecommerce.gold.fact_orders VERSION AS OF 0) AS first_load_rows;

-- 5. Audit SCD-2 seller dimension history (no time travel needed — history is in the table).
-- dim_seller already stores history as rows, so we just filter to the expired versions
-- (is_current = false) to see past seller locations. No VERSION/TIMESTAMP AS OF required.
SELECT seller_id, seller_city, seller_state, effective_from, effective_to, is_current
FROM ecommerce.gold.dim_seller
WHERE is_current = false
ORDER BY seller_id, effective_from;

-- 6. Compare a Silver table before/after the most recent overwrite.
-- Silver tables are overwritten each load, so version 0 is the "before" snapshot and the
-- live table is "after". UNION ALL stacks both counts into one before/after result set.
SELECT 'before' AS v, COUNT(*) AS rows FROM ecommerce.silver.orders VERSION AS OF 0
UNION ALL
SELECT 'after'  AS v, COUNT(*) AS rows FROM ecommerce.silver.orders;

-- 7. RESTORE a table to a previous good version (DESTRUCTIVE — uncomment to use).
-- RESTORE rewrites the table back to an earlier version to recover from a bad load.
-- Left commented because it changes the live table; uncomment only when you really mean it.
-- RESTORE TABLE ecommerce.gold.fact_orders TO VERSION AS OF 3;

-- 8. Control the time-travel retention window.
-- VACUUM permanently deletes old data files, which limits how far back time travel can
-- reach. The SET line disables the safety check that blocks retention under 7 days.
-- SET spark.databricks.delta.retentionDurationCheck.enabled = false;  -- only if shortening below 7 days
-- VACUUM ecommerce.gold.fact_orders RETAIN 168 HOURS;   -- keep 7 days of history

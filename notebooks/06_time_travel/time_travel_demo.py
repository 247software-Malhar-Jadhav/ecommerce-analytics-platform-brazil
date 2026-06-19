# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Delta Time Travel — Demo & Auditing
# MAGIC Demonstrates the spec's required **time travelling feature** on Delta Lake. Useful for:
# MAGIC * Auditing what the data looked like before a daily load (`VERSION AS OF` / `TIMESTAMP AS OF`)
# MAGIC * Recovering from a bad load (`RESTORE`)
# MAGIC * Comparing a seller's SCD-2 history across versions
# MAGIC * Reproducible reporting ("revenue as reported on 2026-06-18")

# COMMAND ----------

# MAGIC %run ../04_utils/common_functions

# COMMAND ----------

cfg = load_config()
cat = cfg["environment"]["catalog"]
gold = cfg["environment"]["gold_schema"]
silver = cfg["environment"]["silver_schema"]
fact = f"{cat}.{gold}.fact_orders"
seller = f"{cat}.{gold}.dim_seller"

# COMMAND ----------

# MAGIC %md ### 1. Inspect the version history

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {fact}"))

# COMMAND ----------

# MAGIC %md ### 2. Read a previous VERSION (PySpark helper)

# COMMAND ----------

latest_version = spark.sql(f"DESCRIBE HISTORY {fact}").agg(F.max("version")).collect()[0][0]
print(f"Latest version: {latest_version}")

if latest_version and latest_version > 0:
    prev = read_delta_version(spark, fact, version=latest_version - 1)
    curr = read_delta(spark, fact)
    print(f"Rows added in last load: {curr.count() - prev.count():,}")

# COMMAND ----------

# MAGIC %md ### 3. Read AS OF a timestamp (SQL)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Revenue exactly as it would have been reported yesterday.
# MAGIC -- Replace the timestamp with any point inside the table's retention window.
# MAGIC SELECT 'as_of_yesterday' AS snapshot, ROUND(SUM(payment_value), 2) AS revenue
# MAGIC FROM ecommerce.gold.fact_orders TIMESTAMP AS OF date_sub(current_date(), 1)
# MAGIC UNION ALL
# MAGIC SELECT 'current' AS snapshot, ROUND(SUM(payment_value), 2) AS revenue
# MAGIC FROM ecommerce.gold.fact_orders;

# COMMAND ----------

# MAGIC %md ### 4. Audit an SCD-2 seller across versions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Full location history for a seller straight from the SCD-2 dimension.
# MAGIC SELECT seller_id, seller_city, seller_state, effective_from, effective_to, is_current
# MAGIC FROM ecommerce.gold.dim_seller
# MAGIC ORDER BY seller_id, effective_from
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md ### 5. Recover from a bad load with RESTORE (commented — destructive)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- If a daily load corrupted the fact, roll the table back to the prior good version:
# MAGIC -- RESTORE TABLE ecommerce.gold.fact_orders TO VERSION AS OF <good_version>;
# MAGIC --
# MAGIC -- VACUUM controls how far back time travel can reach (default 7 days retention):
# MAGIC -- VACUUM ecommerce.gold.fact_orders RETAIN 168 HOURS;

# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Data Quality / Unit Testing
# MAGIC Implements the spec's "Unit Testing / Data Quality Steps":
# MAGIC 1. Row count validation   2. Null check on PK columns
# MAGIC 3. Duplicate record check  4. Schema validation
# MAGIC
# MAGIC Runs as the final Workflow task. Collects all results into `gold.dq_results` and
# MAGIC **fails the job** if any FATAL check fails, so bad data never reaches BI.

# COMMAND ----------

# MAGIC %run ../04_utils/common_functions

# COMMAND ----------

# Load the project config (catalog/schema names live in one place so we never hard-code them).
cfg = load_config()
cat = cfg["environment"]["catalog"]        # Unity Catalog name, e.g. "ecommerce"
gold = cfg["environment"]["gold_schema"]   # Gold (BI-ready) schema name
silver = cfg["environment"]["silver_schema"]  # Silver (cleaned) schema name

# Every check appends one row here so we can save them all and decide pass/fail at the end.
results = []   # (table, check, status, detail, severity)

# Small helper that turns a boolean result into a tidy record.
# severity "FATAL" means a failure should stop the job; "WARN" means just log it.
def record(table, check, passed, detail, severity="FATAL"):
    results.append((table, check, "PASS" if passed else "FAIL", detail, severity))

# COMMAND ----------

# MAGIC %md ### 1. Row count validation — no Gold table should be empty

# COMMAND ----------

# Loop over every Gold table and confirm it actually has rows.
# An empty table usually means an upstream load silently failed.
gold_tables = ["dim_customer", "dim_product", "dim_seller", "dim_category", "dim_date", "fact_orders"]
for t in gold_tables:
    fq = f"{cat}.{gold}.{t}"          # fully qualified name: catalog.schema.table
    cnt = spark.table(fq).count()    # count rows in the table
    record(fq, "row_count > 0", cnt > 0, f"rows={cnt}")

# COMMAND ----------

# MAGIC %md ### 2. Null check on PK / surrogate-key columns

# COMMAND ----------

# Each table's primary/surrogate key must never be NULL — a NULL key breaks joins
# and means a row cannot be uniquely identified.
pk_map = {
    "dim_customer": "customer_sk",
    "dim_product": "product_sk",
    "dim_seller": "seller_sk",
    "dim_category": "category_sk",
    "dim_date": "date_id",
    "fact_orders": "order_id",
}
for t, pk in pk_map.items():
    fq = f"{cat}.{gold}.{t}"
    # Keep only rows where the key is NULL and count them; we want this to be zero.
    nulls = spark.table(fq).filter(F.col(pk).isNull()).count()
    record(fq, f"no null {pk}", nulls == 0, f"null_count={nulls}")

# COMMAND ----------

# MAGIC %md ### 3. Duplicate record check (business / grain keys)

# COMMAND ----------

# Each table has a "grain" — the column(s) that should uniquely identify a row.
# We check that the table's grain has no duplicates.
dup_checks = {
    "dim_customer": ["customer_id"],
    "dim_product": ["product_id"],
    "dim_category": ["product_category_name"],
    "dim_date": ["date_id"],
    "fact_orders": ["order_id", "order_item_id"],   # one row per item within an order
}
for t, keys in dup_checks.items():
    fq = f"{cat}.{gold}.{t}"
    total = spark.table(fq).count()                          # total rows
    distinct = spark.table(fq).select(*keys).distinct().count()  # unique grain values
    # If total != distinct, some grain values repeat (duplicates exist).
    record(fq, f"unique {keys}", total == distinct, f"rows={total} distinct={distinct}")

# dim_seller is a Slowly Changing Dimension type 2 (SCD-2): it keeps history,
# so a seller can have many rows but only ONE may be flagged is_current = True.
# Here we group by seller_id over the current rows and look for any seller with >1.
seller_fq = f"{cat}.{gold}.dim_seller"
dup_current = (
    spark.table(seller_fq).filter(F.col("is_current") == True)  # noqa: E712
    .groupBy("seller_id").count().filter(F.col("count") > 1).count()
)
record(seller_fq, "one current row per seller_id (SCD-2)", dup_current == 0, f"violations={dup_current}")

# COMMAND ----------

# MAGIC %md ### 4. Schema validation — required columns present in fact

# COMMAND ----------

# Confirm the fact table still has all the columns BI/reports depend on.
# A schema drift (renamed/dropped column upstream) would break dashboards.
required_fact_cols = {
    "order_id", "order_item_id", "product_id", "customer_id", "seller_id",
    "date_id", "order_status", "price", "freight_value", "payment_value",
    "review_score", "delivery_days",
}
fact_cols = set(spark.table(f"{cat}.{gold}.fact_orders").columns)
# Set difference: required columns that are NOT present in the actual table.
missing = required_fact_cols - fact_cols
record(f"{cat}.{gold}.fact_orders", "required columns present", len(missing) == 0, f"missing={sorted(missing)}")

# COMMAND ----------

# MAGIC %md ### Referential integrity — fact FKs resolve to dimensions

# COMMAND ----------

# Referential integrity: every customer_id in the fact should exist in dim_customer.
# A "left_anti" join keeps only fact rows that have NO match in the dimension —
# those are "orphans" (foreign keys pointing at nothing). Flagged as WARN, not FATAL.
fact = spark.table(f"{cat}.{gold}.fact_orders")
dim_cust = spark.table(f"{cat}.{gold}.dim_customer").select("customer_id")
orphans = fact.join(dim_cust, "customer_id", "left_anti").count()
record(f"{cat}.{gold}.fact_orders", "no orphan customer_id", orphans == 0, f"orphans={orphans}", severity="WARN")

# COMMAND ----------

# MAGIC %md ### Persist results & fail on FATAL

# COMMAND ----------

# Turn the collected results into a DataFrame, stamp it with the run time,
# and APPEND to gold.dq_results so we keep a history of every run's checks.
schema = "table_name string, check_name string, status string, detail string, severity string"
dq_df = spark.createDataFrame(results, schema).withColumn("run_ts", F.current_timestamp())
write_delta(dq_df, f"{cat}.{gold}.dq_results", mode="append")
display(dq_df)

# Find any check that FAILED and is marked FATAL.
fatal_failures = [r for r in results if r[2] == "FAIL" and r[4] == "FATAL"]
# Raising an exception fails the Databricks job, so bad data never reaches BI.
if fatal_failures:
    raise Exception(f"DATA QUALITY GATE FAILED — {len(fatal_failures)} fatal checks: {fatal_failures}")
print("All FATAL data quality checks passed.")

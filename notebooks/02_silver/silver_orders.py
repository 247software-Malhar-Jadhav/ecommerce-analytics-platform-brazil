# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — orders
# MAGIC * Cast all 5 timestamp columns (`order_purchase_timestamp` etc.) to timestamp
# MAGIC * Keep nullable delivery timestamps as-is (legitimately null for non-delivered orders)
# MAGIC * Standardize `order_status` to lowercase
# MAGIC * Dedup latest per `order_id`, drop null PKs, add `updated_ts`, overwrite

# COMMAND ----------

# Bring in shared helpers (read_delta, deduplicate, drop_null_keys, add_audit_columns, F, ...).
# MAGIC %run ../04_utils/common_functions

# COMMAND ----------

# Resolve Bronze source and Silver target tables from config (raw in, cleaned out).
cfg = load_config()
cat = cfg["environment"]["catalog"]
src = f'{cat}.{cfg["environment"]["bronze_schema"]}.orders'   # raw input
tgt = f'{cat}.{cfg["environment"]["silver_schema"]}.orders'   # cleaned output

# Load the raw Bronze orders table.
bronze = read_delta(spark, src)

# The string pattern the CSV timestamps are stored in; used to parse text -> real timestamps.
TS_FMT = "yyyy-MM-dd HH:mm:ss"
# All five date/time columns that arrived as strings and must become true timestamp types.
ts_cols = [
    "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
    "order_delivered_customer_date", "order_estimated_delivery_date",
]

# COMMAND ----------

# Clean step 1: dedup to the latest row per order, drop null PKs, standardize status text.
clean = (
    bronze
    # Keep newest row per order_id (latest ingest_ts) — collapse Bronze history to current state.
    .transform(lambda d: deduplicate(d, "order_id", order_col="ingest_ts"))
    # Remove rows with no order_id; the primary key is required.
    .transform(lambda d: drop_null_keys(d, "order_id"))
    # Standardize status casing so "Delivered" and "delivered" group as one value.
    .withColumn("order_status", F.lower(F.trim(F.col("order_status"))))
)
# Clean step 2: convert each date column from string to a real timestamp type.
# F.to_timestamp returns null if the text doesn't match TS_FMT; delivery dates are
# legitimately null for orders that were never delivered, so that's expected.
for c in ts_cols:
    clean = clean.withColumn(c, F.to_timestamp(F.col(c), TS_FMT))

# Drop Bronze-only columns, then add audit columns (e.g. updated_ts).
clean = add_audit_columns(clean.drop("_rescued_data", "source_file"))

# COMMAND ----------

# DQ: purchase timestamp is mandatory; estimated delivery should not precede purchase
# Hard check: no order may be missing its primary key (fails the notebook if violated).
assert clean.filter(F.col("order_id").isNull()).count() == 0, "Null order_id"
# Soft check: count logically impossible dates (estimated delivery before purchase).
# These rows are KEPT (just reported), not dropped — Silver flags anomalies rather than
# silently deleting data that downstream analysts may still want to inspect.
bad_dates = clean.filter(
    F.col("order_estimated_delivery_date").isNotNull()
    & F.col("order_purchase_timestamp").isNotNull()
    & (F.col("order_estimated_delivery_date") < F.col("order_purchase_timestamp"))
).count()
print(f"Orders with estimated-delivery-before-purchase (kept, flagged): {bad_dates}")

# Overwrite Silver with the full current clean state.
write_delta(clean, tgt, mode="overwrite")
spark.sql(f"COMMENT ON TABLE {tgt} IS 'Silver cleaned orders: timestamps cast, status lowercased, deduped.'")
print(f"Silver orders rows: {clean.count():,}")
display(clean.limit(5))

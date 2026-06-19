# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — orders
# MAGIC * Cast all 5 timestamp columns (`order_purchase_timestamp` etc.) to timestamp
# MAGIC * Keep nullable delivery timestamps as-is (legitimately null for non-delivered orders)
# MAGIC * Standardize `order_status` to lowercase
# MAGIC * Dedup latest per `order_id`, drop null PKs, add `updated_ts`, overwrite

# COMMAND ----------

# MAGIC %run ../04_utils/common_functions

# COMMAND ----------

cfg = load_config()
cat = cfg["environment"]["catalog"]
src = f'{cat}.{cfg["environment"]["bronze_schema"]}.orders'
tgt = f'{cat}.{cfg["environment"]["silver_schema"]}.orders'

bronze = read_delta(spark, src)

TS_FMT = "yyyy-MM-dd HH:mm:ss"
ts_cols = [
    "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
    "order_delivered_customer_date", "order_estimated_delivery_date",
]

# COMMAND ----------

clean = (
    bronze
    .transform(lambda d: deduplicate(d, "order_id", order_col="ingest_ts"))
    .transform(lambda d: drop_null_keys(d, "order_id"))
    .withColumn("order_status", F.lower(F.trim(F.col("order_status"))))
)
for c in ts_cols:
    clean = clean.withColumn(c, F.to_timestamp(F.col(c), TS_FMT))

clean = add_audit_columns(clean.drop("_rescued_data", "source_file"))

# COMMAND ----------

# DQ: purchase timestamp is mandatory; estimated delivery should not precede purchase
assert clean.filter(F.col("order_id").isNull()).count() == 0, "Null order_id"
bad_dates = clean.filter(
    F.col("order_estimated_delivery_date").isNotNull()
    & F.col("order_purchase_timestamp").isNotNull()
    & (F.col("order_estimated_delivery_date") < F.col("order_purchase_timestamp"))
).count()
print(f"Orders with estimated-delivery-before-purchase (kept, flagged): {bad_dates}")

write_delta(clean, tgt, mode="overwrite")
spark.sql(f"COMMENT ON TABLE {tgt} IS 'Silver cleaned orders: timestamps cast, status lowercased, deduped.'")
print(f"Silver orders rows: {clean.count():,}")
display(clean.limit(5))

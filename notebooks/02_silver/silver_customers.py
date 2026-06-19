# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — customers
# MAGIC Clean & standardize customers. Actions (from spec):
# MAGIC * `customer_zip_code_prefix` → remove spaces, cast to int
# MAGIC * `customer_city` → lowercase + trim
# MAGIC * `customer_state` → uppercase (capitalize state code)
# MAGIC * dedup to latest row per `customer_id` (incremental), drop null PKs, add `updated_ts`
# MAGIC * overwrite mode

# COMMAND ----------

# MAGIC %run ../04_utils/common_functions

# COMMAND ----------

cfg = load_config()
cat = cfg["environment"]["catalog"]
src = f'{cat}.{cfg["environment"]["bronze_schema"]}.customers'
tgt = f'{cat}.{cfg["environment"]["silver_schema"]}.customers'

bronze = read_delta(spark, src)

# COMMAND ----------

clean = (
    bronze
    .transform(lambda d: deduplicate(d, "customer_id", order_col="ingest_ts"))
    .transform(lambda d: drop_null_keys(d, "customer_id"))
    .withColumn("customer_zip_code_prefix",
                F.regexp_replace(F.col("customer_zip_code_prefix"), r"\s+", "").cast("int"))
    .withColumn("customer_city", F.lower(F.trim(F.col("customer_city"))))
    .withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))
    .transform(trim_strings)
    .drop("_rescued_data", "source_file")
)
clean = add_audit_columns(clean)

# COMMAND ----------

# Data quality: PK must be unique and non-null
assert clean.filter(F.col("customer_id").isNull()).count() == 0, "Null customer_id in Silver"
assert clean.count() == clean.select("customer_id").distinct().count(), "Duplicate customer_id in Silver"

write_delta(clean, tgt, mode="overwrite")
spark.sql(f"COMMENT ON TABLE {tgt} IS 'Silver cleaned customers: zip->int, city lowercased, state uppercased, deduped.'")
print(f"Silver customers rows: {clean.count():,}")
display(clean.limit(5))

# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — sellers
# MAGIC * `seller_zip_code_prefix` → int (strip spaces)
# MAGIC * `seller_city` → lowercase+trim, `seller_state` → uppercase
# MAGIC * Dedup latest per `seller_id`, overwrite
# MAGIC
# MAGIC NOTE: SCD-2 history tracking happens in **Gold** (`gold_dim_seller_scd2`). Silver simply
# MAGIC provides the latest clean snapshot of each seller; Gold compares it to the existing
# MAGIC dimension and expires/inserts rows when `seller_city`/`seller_state` change.

# COMMAND ----------

# MAGIC %run ../04_utils/common_functions

# COMMAND ----------

cfg = load_config()
cat = cfg["environment"]["catalog"]
src = f'{cat}.{cfg["environment"]["bronze_schema"]}.sellers'
tgt = f'{cat}.{cfg["environment"]["silver_schema"]}.sellers'

bronze = read_delta(spark, src)

# COMMAND ----------

clean = (
    bronze
    .transform(lambda d: deduplicate(d, "seller_id", order_col="ingest_ts"))
    .transform(lambda d: drop_null_keys(d, "seller_id"))
    .withColumn("seller_zip_code_prefix",
                F.regexp_replace(F.col("seller_zip_code_prefix"), r"\s+", "").cast("int"))
    .withColumn("seller_city", F.lower(F.trim(F.col("seller_city"))))
    .withColumn("seller_state", F.upper(F.trim(F.col("seller_state"))))
    .drop("_rescued_data", "source_file")
)
clean = add_audit_columns(clean)

# COMMAND ----------

assert clean.count() == clean.select("seller_id").distinct().count(), "Duplicate seller_id"
write_delta(clean, tgt, mode="overwrite")
spark.sql(f"COMMENT ON TABLE {tgt} IS 'Silver sellers: zip->int, city lowercased, state uppercased, deduped. SCD-2 applied in Gold.'")
print(f"Silver sellers rows: {clean.count():,}")
display(clean.limit(5))

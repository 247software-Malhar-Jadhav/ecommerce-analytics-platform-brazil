# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — category_translation
# MAGIC Small static lookup (Portuguese -> English category name). Trim/lowercase the
# MAGIC Portuguese key so it joins cleanly to `silver.products.product_category_name`.
# MAGIC overwrite (full reload, static reference data).

# COMMAND ----------

# MAGIC %run ../04_utils/common_functions

# COMMAND ----------

cfg = load_config()
cat = cfg["environment"]["catalog"]
src = f'{cat}.{cfg["environment"]["bronze_schema"]}.category_translation'
tgt = f'{cat}.{cfg["environment"]["silver_schema"]}.category_translation'

bronze = read_delta(spark, src)

# COMMAND ----------

clean = (
    bronze
    .transform(lambda d: deduplicate(d, "product_category_name", order_col="ingest_ts"))
    .transform(lambda d: drop_null_keys(d, "product_category_name"))
    .withColumn("product_category_name", F.lower(F.trim(F.col("product_category_name"))))
    .withColumn("product_category_name_english", F.lower(F.trim(F.col("product_category_name_english"))))
    .drop("_rescued_data", "source_file")
)
clean = add_audit_columns(clean)

# COMMAND ----------

write_delta(clean, tgt, mode="overwrite")
spark.sql(f"COMMENT ON TABLE {tgt} IS 'Silver category_translation: normalized PT->EN lookup, deduped.'")
print(f"Silver category_translation rows: {clean.count():,}")
display(clean.limit(10))

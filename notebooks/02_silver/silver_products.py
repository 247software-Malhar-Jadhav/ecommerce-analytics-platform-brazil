# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — products
# MAGIC * Fix the raw Olist column typos `product_name_lenght` -> `product_name_length` etc.
# MAGIC * Cast all measurement columns to int and validate they are non-negative
# MAGIC * Lowercase/trim `product_category_name` so the dim_category join is reliable
# MAGIC * Dedup latest per `product_id`, overwrite

# COMMAND ----------

# MAGIC %run ../04_utils/common_functions

# COMMAND ----------

cfg = load_config()
cat = cfg["environment"]["catalog"]
src = f'{cat}.{cfg["environment"]["bronze_schema"]}.products'
tgt = f'{cat}.{cfg["environment"]["silver_schema"]}.products'

bronze = read_delta(spark, src)

int_cols = {
    "product_name_lenght": "product_name_length",
    "product_description_lenght": "product_description_length",
    "product_photos_qty": "product_photos_qty",
    "product_weight_g": "product_weight_g",
    "product_length_cm": "product_length_cm",
    "product_height_cm": "product_height_cm",
    "product_width_cm": "product_width_cm",
}

# COMMAND ----------

clean = (
    bronze
    .transform(lambda d: deduplicate(d, "product_id", order_col="ingest_ts"))
    .transform(lambda d: drop_null_keys(d, "product_id"))
    .withColumn("product_category_name", F.lower(F.trim(F.col("product_category_name"))))
)
for raw_name, fixed_name in int_cols.items():
    clean = clean.withColumn(fixed_name, F.col(raw_name).cast("int"))
    if raw_name != fixed_name:
        clean = clean.drop(raw_name)

# Validate measurements: negatives are invalid -> set null (kept, not dropped, so product survives)
for c in ["product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm",
          "product_photos_qty", "product_name_length", "product_description_length"]:
    clean = clean.withColumn(c, F.when(F.col(c) < 0, None).otherwise(F.col(c)))

clean = add_audit_columns(clean.drop("_rescued_data", "source_file"))

# COMMAND ----------

write_delta(clean, tgt, mode="overwrite")
spark.sql(f"COMMENT ON TABLE {tgt} IS 'Silver products: lenght typo fixed, ints cast/validated, category normalized, deduped.'")
print(f"Silver products rows: {clean.count():,}")
display(clean.limit(5))

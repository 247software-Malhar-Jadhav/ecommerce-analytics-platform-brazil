# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — category_translation
# MAGIC Small static lookup (Portuguese -> English category name). Trim/lowercase the
# MAGIC Portuguese key so it joins cleanly to `silver.products.product_category_name`.
# MAGIC overwrite (full reload, static reference data).

# COMMAND ----------

# MAGIC %run ../04_utils/common_functions

# COMMAND ----------

# Load project settings and build fully-qualified table names (catalog.schema.table).
cfg = load_config()
cat = cfg["environment"]["catalog"]
src = f'{cat}.{cfg["environment"]["bronze_schema"]}.category_translation'    # Bronze input
tgt = f'{cat}.{cfg["environment"]["silver_schema"]}.category_translation'   # Silver output

# Read the Bronze category-translation lookup table.
bronze = read_delta(spark, src)

# COMMAND ----------

# Build the cleaned Silver lookup. The Portuguese category name is the key.
clean = (
    bronze
    # Keep newest row per category name to drop any re-ingested duplicates.
    .transform(lambda d: deduplicate(d, "product_category_name", order_col="ingest_ts"))
    # Drop rows missing the key.
    .transform(lambda d: drop_null_keys(d, "product_category_name"))
    # Lowercase + trim the PT key so it matches silver.products.product_category_name exactly.
    .withColumn("product_category_name", F.lower(F.trim(F.col("product_category_name"))))
    # Normalize the English translation the same way for consistency.
    .withColumn("product_category_name_english", F.lower(F.trim(F.col("product_category_name_english"))))
    # Remove Bronze-only bookkeeping columns.
    .drop("_rescued_data", "source_file")
)
# Add standard audit columns for lineage.
clean = add_audit_columns(clean)

# COMMAND ----------

# Overwrite the small static lookup (full reload) and document it in the catalog.
write_delta(clean, tgt, mode="overwrite")
spark.sql(f"COMMENT ON TABLE {tgt} IS 'Silver category_translation: normalized PT->EN lookup, deduped.'")
# Sanity check: row count and a preview of the mappings.
print(f"Silver category_translation rows: {clean.count():,}")
display(clean.limit(10))

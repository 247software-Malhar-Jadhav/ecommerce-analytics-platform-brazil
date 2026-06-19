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

# Load project settings and build fully-qualified table names (catalog.schema.table).
cfg = load_config()
cat = cfg["environment"]["catalog"]
src = f'{cat}.{cfg["environment"]["bronze_schema"]}.products'    # raw-but-typed Bronze input
tgt = f'{cat}.{cfg["environment"]["silver_schema"]}.products'   # cleaned Silver output

# Read the Bronze products Delta table.
bronze = read_delta(spark, src)

# Map raw column name -> desired (fixed) column name. The Olist source CSV misspells
# "length" as "lenght"; this mapping both renames the typos and lists every numeric
# measurement column we need to cast to int. (Keys equal to values are just cast, not renamed.)
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

# First cleaning pass: dedupe, enforce the primary key, and normalize the category text.
clean = (
    bronze
    # Keep newest row per product_id (latest ingest_ts) to remove re-ingested duplicates.
    .transform(lambda d: deduplicate(d, "product_id", order_col="ingest_ts"))
    # Drop rows missing the product_id primary key.
    .transform(lambda d: drop_null_keys(d, "product_id"))
    # Lowercase + trim the category name so it joins reliably to the dim_category lookup later.
    .withColumn("product_category_name", F.lower(F.trim(F.col("product_category_name"))))
)
# Cast each measurement column to int (and rename the typo'd source columns to the fixed name).
for raw_name, fixed_name in int_cols.items():
    clean = clean.withColumn(fixed_name, F.col(raw_name).cast("int"))
    # If the name actually changed, drop the old misspelled column to avoid duplicates.
    if raw_name != fixed_name:
        clean = clean.drop(raw_name)

# Validate measurements: negatives are invalid -> set null (kept, not dropped, so product survives)
# We null out bad values rather than dropping the whole product, so it still appears in reports.
for c in ["product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm",
          "product_photos_qty", "product_name_length", "product_description_length"]:
    clean = clean.withColumn(c, F.when(F.col(c) < 0, None).otherwise(F.col(c)))

# Drop Bronze-only bookkeeping columns and add standard audit columns for lineage.
clean = add_audit_columns(clean.drop("_rescued_data", "source_file"))

# COMMAND ----------

# Overwrite the Silver products table (full refresh) and document it in the catalog.
write_delta(clean, tgt, mode="overwrite")
spark.sql(f"COMMENT ON TABLE {tgt} IS 'Silver products: lenght typo fixed, ints cast/validated, category normalized, deduped.'")
# Sanity check: print row count and preview a few rows to eyeball the result.
print(f"Silver products rows: {clean.count():,}")
display(clean.limit(5))

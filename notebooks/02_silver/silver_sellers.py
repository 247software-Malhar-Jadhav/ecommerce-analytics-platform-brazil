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

# Load project settings and build fully-qualified table names (catalog.schema.table).
cfg = load_config()
cat = cfg["environment"]["catalog"]
src = f'{cat}.{cfg["environment"]["bronze_schema"]}.sellers'    # Bronze input
tgt = f'{cat}.{cfg["environment"]["silver_schema"]}.sellers'   # Silver output

# Read the Bronze sellers Delta table.
bronze = read_delta(spark, src)

# COMMAND ----------

# Build the cleaned Silver sellers DataFrame.
clean = (
    bronze
    # Keep newest row per seller_id to drop re-ingested duplicates.
    .transform(lambda d: deduplicate(d, "seller_id", order_col="ingest_ts"))
    # Drop rows missing the seller_id primary key.
    .transform(lambda d: drop_null_keys(d, "seller_id"))
    # Strip any whitespace from the zip prefix, then cast to int for a clean numeric key.
    .withColumn("seller_zip_code_prefix",
                F.regexp_replace(F.col("seller_zip_code_prefix"), r"\s+", "").cast("int"))
    # Normalize text: city to lowercase, state to uppercase (e.g. "SP"), so values are consistent.
    .withColumn("seller_city", F.lower(F.trim(F.col("seller_city"))))
    .withColumn("seller_state", F.upper(F.trim(F.col("seller_state"))))
    # Remove Bronze-only bookkeeping columns.
    .drop("_rescued_data", "source_file")
)
# Add standard audit columns for lineage/traceability.
clean = add_audit_columns(clean)

# COMMAND ----------

# Data-quality gate: confirm seller_id is unique (row count == distinct id count) before writing.
assert clean.count() == clean.select("seller_id").distinct().count(), "Duplicate seller_id"
# Overwrite the Silver sellers table (full refresh) and document it in the catalog.
write_delta(clean, tgt, mode="overwrite")
spark.sql(f"COMMENT ON TABLE {tgt} IS 'Silver sellers: zip->int, city lowercased, state uppercased, deduped. SCD-2 applied in Gold.'")
# Sanity check: row count and a small preview.
print(f"Silver sellers rows: {clean.count():,}")
display(clean.limit(5))

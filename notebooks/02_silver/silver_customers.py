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

# Bring in shared helpers (read_delta, deduplicate, drop_null_keys, add_audit_columns, F, ...).
# MAGIC %run ../04_utils/common_functions

# COMMAND ----------

# Resolve the Bronze source table and Silver target table from central config.
# Silver reads from Bronze (raw) and writes a cleaned, standardized version.
cfg = load_config()
cat = cfg["environment"]["catalog"]
src = f'{cat}.{cfg["environment"]["bronze_schema"]}.customers'   # raw input
tgt = f'{cat}.{cfg["environment"]["silver_schema"]}.customers'   # cleaned output

# Load the raw Bronze customers table as a Spark DataFrame.
bronze = read_delta(spark, src)

# COMMAND ----------

# Build the cleaned DataFrame as a chain of transformations (read top to bottom).
# .transform(...) just applies a helper function to the DataFrame and returns a new one.
clean = (
    bronze
    # Keep only the newest row per customer_id (latest ingest_ts wins) — Bronze keeps every
    # historical snapshot, so Silver collapses to one current record per key.
    .transform(lambda d: deduplicate(d, "customer_id", order_col="ingest_ts"))
    # Drop rows missing the primary key; a customer with no id is unusable downstream.
    .transform(lambda d: drop_null_keys(d, "customer_id"))
    # Zip prefix: strip any whitespace then cast text -> integer for consistent typing.
    .withColumn("customer_zip_code_prefix",
                F.regexp_replace(F.col("customer_zip_code_prefix"), r"\s+", "").cast("int"))
    # City: lowercase + trim so "  Sao Paulo " and "sao paulo" become one value (grouping later).
    .withColumn("customer_city", F.lower(F.trim(F.col("customer_city"))))
    # State: uppercase + trim to standardize the two-letter state code (e.g. "sp" -> "SP").
    .withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))
    # Trim leading/trailing spaces on all remaining string columns.
    .transform(trim_strings)
    # Drop Bronze-only bookkeeping columns that Silver consumers don't need.
    .drop("_rescued_data", "source_file")
)
# Add audit columns (e.g. updated_ts) so we can tell when this Silver row was last refreshed.
clean = add_audit_columns(clean)

# COMMAND ----------

# Data quality: PK must be unique and non-null
# These assert checks fail the notebook loudly if the cleaning logic didn't hold, so bad
# data never silently reaches downstream layers (Gold/reporting).
assert clean.filter(F.col("customer_id").isNull()).count() == 0, "Null customer_id in Silver"
assert clean.count() == clean.select("customer_id").distinct().count(), "Duplicate customer_id in Silver"

# Overwrite mode: Silver is a full rebuild of the current clean state each run.
write_delta(clean, tgt, mode="overwrite")
# Document the table and print a row count so the run is self-explanatory.
spark.sql(f"COMMENT ON TABLE {tgt} IS 'Silver cleaned customers: zip->int, city lowercased, state uppercased, deduped.'")
print(f"Silver customers rows: {clean.count():,}")
display(clean.limit(5))

# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — payments
# MAGIC * `payment_type` → remove special chars, keep only `_` (credit@card / credit#card -> credit_card)
# MAGIC * `payment_value` → remove nulls & special chars, cast to double, validate >= 0
# MAGIC * `payment_sequential`, `payment_installments` → int
# MAGIC * Dedup on (`order_id`,`payment_sequential`), overwrite

# COMMAND ----------

# Bring in shared helpers (read_delta, deduplicate, clean_money, clean_special_chars, F, ...).
# MAGIC %run ../04_utils/common_functions

# COMMAND ----------

# Resolve Bronze source and Silver target tables from config (raw in, cleaned out).
cfg = load_config()
cat = cfg["environment"]["catalog"]
src = f'{cat}.{cfg["environment"]["bronze_schema"]}.payments'   # raw input
tgt = f'{cat}.{cfg["environment"]["silver_schema"]}.payments'   # cleaned output

# Load the raw Bronze payments table.
bronze = read_delta(spark, src)

# COMMAND ----------

# Build the cleaned DataFrame as a transformation chain (read top to bottom).
clean = (
    bronze
    # Composite key: an order can have several payment rows (e.g. split payments), so
    # order_id + payment_sequential together identify one unique payment; keep the latest.
    .transform(lambda d: deduplicate(d, ["order_id", "payment_sequential"], order_col="ingest_ts"))
    # Drop rows missing order_id.
    .transform(lambda d: drop_null_keys(d, "order_id"))
    # Cast the sequence and installment counts from string to int.
    .withColumn("payment_sequential", F.col("payment_sequential").cast("int"))
    .withColumn("payment_installments", F.col("payment_installments").cast("int"))
    # Normalize payment_type: strip special chars (e.g. "credit@card" -> "credit_card")
    # so categories are consistent for later grouping/reporting.
    .withColumn("payment_type", clean_special_chars("payment_type"))
    # Strip currency symbols and cast the amount to double.
    .withColumn("payment_value", clean_money("payment_value"))
    # Keep only valid amounts: non-null and not negative (a payment can't be below zero).
    .filter(F.col("payment_value").isNotNull() & (F.col("payment_value") >= 0))
    .drop("_rescued_data", "source_file")
)
# Add audit columns (e.g. updated_ts) marking when this Silver row was refreshed.
clean = add_audit_columns(clean)

# COMMAND ----------

# Overwrite Silver with the full current clean state, then document and summarize.
write_delta(clean, tgt, mode="overwrite")
spark.sql(f"COMMENT ON TABLE {tgt} IS 'Silver payments: payment_type normalized, value cleaned/validated, deduped.'")
print(f"Silver payments rows: {clean.count():,}")
# Quick distribution check: count rows per payment_type, most common first.
display(clean.groupBy("payment_type").count().orderBy(F.desc("count")))

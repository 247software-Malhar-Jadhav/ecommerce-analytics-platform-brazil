# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — order_items
# MAGIC * `price` & `freight_value` → strip `$` and cast to double
# MAGIC * `order_item_id` → int, `shipping_limit_date` → timestamp
# MAGIC * Validate numeric ranges (price >= 0, freight >= 0); route invalid rows to error table
# MAGIC * Dedup on (`order_id`,`order_item_id`), overwrite

# COMMAND ----------

# Bring in shared helpers (read_delta, deduplicate, drop_null_keys, clean_money, F, ...).
# MAGIC %run ../04_utils/common_functions

# COMMAND ----------

# Resolve table names. Note the extra 'err' table: rows that fail validation are
# quarantined there instead of being thrown away (a "dead-letter" pattern).
cfg = load_config()
cat = cfg["environment"]["catalog"]
src = f'{cat}.{cfg["environment"]["bronze_schema"]}.order_items'        # raw input
tgt = f'{cat}.{cfg["environment"]["silver_schema"]}.order_items'        # clean output
err = f'{cat}.{cfg["environment"]["silver_schema"]}.order_items_errors' # quarantined bad rows

# Load the raw Bronze order_items table.
bronze = read_delta(spark, src)

# COMMAND ----------

# Build a typed DataFrame: dedup, drop null PKs, and fix column types.
# The primary key here is a COMPOSITE key (order_id + order_item_id) — one order has
# multiple line items, so both columns together identify a unique row.
typed = (
    bronze
    .transform(lambda d: deduplicate(d, ["order_id", "order_item_id"], order_col="ingest_ts"))
    .transform(lambda d: drop_null_keys(d, ["order_id", "order_item_id"]))
    # Cast the line-item sequence number from string to int.
    .withColumn("order_item_id", F.col("order_item_id").cast("int"))
    # Parse the shipping deadline string into a real timestamp.
    .withColumn("shipping_limit_date", F.to_timestamp("shipping_limit_date", "yyyy-MM-dd HH:mm:ss"))
    # clean_money strips currency symbols like "$" and casts the value to double.
    .withColumn("price", clean_money("price"))
    .withColumn("freight_value", clean_money("freight_value"))
    .drop("_rescued_data", "source_file")
)

# Validate numeric ranges. Negative or null money is invalid for a sold item.
# valid_cond is a boolean column expression evaluated per row (True = good row).
valid_cond = (F.col("price").isNotNull() & (F.col("price") >= 0) &
              F.col("freight_value").isNotNull() & (F.col("freight_value") >= 0))

# Split the data: rows passing the check become Silver; the rest are quarantined with a reason.
# ~valid_cond means "NOT valid". This keeps bad data for investigation instead of losing it.
clean = add_audit_columns(typed.filter(valid_cond))
errors = add_audit_columns(typed.filter(~valid_cond)).withColumn("error_reason", F.lit("price/freight null or negative"))

# COMMAND ----------

# Write the good rows to the Silver table (full overwrite).
write_delta(clean, tgt, mode="overwrite")
# Only write the error table if there were actually bad rows, to avoid creating an empty table.
if errors.count() > 0:
    write_delta(errors, err, mode="overwrite")
spark.sql(f"COMMENT ON TABLE {tgt} IS 'Silver order_items: $ stripped, casts, range-validated, deduped.'")
print(f"Silver order_items rows: {clean.count():,} | quarantined: {errors.count():,}")
display(clean.limit(5))

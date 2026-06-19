# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — order_items
# MAGIC * `price` & `freight_value` → strip `$` and cast to double
# MAGIC * `order_item_id` → int, `shipping_limit_date` → timestamp
# MAGIC * Validate numeric ranges (price >= 0, freight >= 0); route invalid rows to error table
# MAGIC * Dedup on (`order_id`,`order_item_id`), overwrite

# COMMAND ----------

# MAGIC %run ../04_utils/common_functions

# COMMAND ----------

cfg = load_config()
cat = cfg["environment"]["catalog"]
src = f'{cat}.{cfg["environment"]["bronze_schema"]}.order_items'
tgt = f'{cat}.{cfg["environment"]["silver_schema"]}.order_items'
err = f'{cat}.{cfg["environment"]["silver_schema"]}.order_items_errors'

bronze = read_delta(spark, src)

# COMMAND ----------

typed = (
    bronze
    .transform(lambda d: deduplicate(d, ["order_id", "order_item_id"], order_col="ingest_ts"))
    .transform(lambda d: drop_null_keys(d, ["order_id", "order_item_id"]))
    .withColumn("order_item_id", F.col("order_item_id").cast("int"))
    .withColumn("shipping_limit_date", F.to_timestamp("shipping_limit_date", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("price", clean_money("price"))
    .withColumn("freight_value", clean_money("freight_value"))
    .drop("_rescued_data", "source_file")
)

# Validate numeric ranges. Negative or null money is invalid for a sold item.
valid_cond = (F.col("price").isNotNull() & (F.col("price") >= 0) &
              F.col("freight_value").isNotNull() & (F.col("freight_value") >= 0))

clean = add_audit_columns(typed.filter(valid_cond))
errors = add_audit_columns(typed.filter(~valid_cond)).withColumn("error_reason", F.lit("price/freight null or negative"))

# COMMAND ----------

write_delta(clean, tgt, mode="overwrite")
if errors.count() > 0:
    write_delta(errors, err, mode="overwrite")
spark.sql(f"COMMENT ON TABLE {tgt} IS 'Silver order_items: $ stripped, casts, range-validated, deduped.'")
print(f"Silver order_items rows: {clean.count():,} | quarantined: {errors.count():,}")
display(clean.limit(5))

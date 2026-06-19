# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — payments
# MAGIC * `payment_type` → remove special chars, keep only `_` (credit@card / credit#card -> credit_card)
# MAGIC * `payment_value` → remove nulls & special chars, cast to double, validate >= 0
# MAGIC * `payment_sequential`, `payment_installments` → int
# MAGIC * Dedup on (`order_id`,`payment_sequential`), overwrite

# COMMAND ----------

# MAGIC %run ../04_utils/common_functions

# COMMAND ----------

cfg = load_config()
cat = cfg["environment"]["catalog"]
src = f'{cat}.{cfg["environment"]["bronze_schema"]}.payments'
tgt = f'{cat}.{cfg["environment"]["silver_schema"]}.payments'

bronze = read_delta(spark, src)

# COMMAND ----------

clean = (
    bronze
    .transform(lambda d: deduplicate(d, ["order_id", "payment_sequential"], order_col="ingest_ts"))
    .transform(lambda d: drop_null_keys(d, "order_id"))
    .withColumn("payment_sequential", F.col("payment_sequential").cast("int"))
    .withColumn("payment_installments", F.col("payment_installments").cast("int"))
    .withColumn("payment_type", clean_special_chars("payment_type"))
    .withColumn("payment_value", clean_money("payment_value"))
    .filter(F.col("payment_value").isNotNull() & (F.col("payment_value") >= 0))
    .drop("_rescued_data", "source_file")
)
clean = add_audit_columns(clean)

# COMMAND ----------

write_delta(clean, tgt, mode="overwrite")
spark.sql(f"COMMENT ON TABLE {tgt} IS 'Silver payments: payment_type normalized, value cleaned/validated, deduped.'")
print(f"Silver payments rows: {clean.count():,}")
display(clean.groupBy("payment_type").count().orderBy(F.desc("count")))

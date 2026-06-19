# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — reviews
# MAGIC * `review_score` → int, default 0 when null/invalid, validate range 0–5
# MAGIC * `review_creation_date`, `review_answer_timestamp` → timestamp (answer nullable)
# MAGIC * Optional comment columns trimmed, kept as-is
# MAGIC * Dedup latest per `review_id`, overwrite

# COMMAND ----------

# MAGIC %run ../04_utils/common_functions

# COMMAND ----------

cfg = load_config()
cat = cfg["environment"]["catalog"]
src = f'{cat}.{cfg["environment"]["bronze_schema"]}.reviews'
tgt = f'{cat}.{cfg["environment"]["silver_schema"]}.reviews'

bronze = read_delta(spark, src)

# COMMAND ----------

clean = (
    bronze
    .transform(lambda d: deduplicate(d, "review_id", order_col="ingest_ts"))
    .transform(lambda d: drop_null_keys(d, "review_id"))
    .withColumn("review_score", F.col("review_score").cast("int"))
    .withColumn("review_score",
                F.when(F.col("review_score").isNull(), F.lit(0))
                 .when((F.col("review_score") < 0) | (F.col("review_score") > 5), F.lit(0))
                 .otherwise(F.col("review_score")))
    .withColumn("review_creation_date", F.to_timestamp("review_creation_date", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("review_answer_timestamp", F.to_timestamp("review_answer_timestamp", "yyyy-MM-dd HH:mm:ss"))
    .transform(trim_strings)
    .drop("_rescued_data", "source_file")
)
clean = add_audit_columns(clean)

# COMMAND ----------

assert clean.filter((F.col("review_score") < 0) | (F.col("review_score") > 5)).count() == 0, "review_score out of range"
write_delta(clean, tgt, mode="overwrite")
spark.sql(f"COMMENT ON TABLE {tgt} IS 'Silver reviews: score defaulted/validated 0-5, dates cast, deduped.'")
print(f"Silver reviews rows: {clean.count():,}")
display(clean.groupBy("review_score").count().orderBy("review_score"))

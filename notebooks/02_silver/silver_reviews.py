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

# Load project settings (catalog/schema names, paths) from the shared config helper.
cfg = load_config()
cat = cfg["environment"]["catalog"]                                   # Unity Catalog name
# Build fully-qualified table names: catalog.schema.table
# src = where we read raw-but-typed data (Bronze); tgt = where we write cleaned data (Silver)
src = f'{cat}.{cfg["environment"]["bronze_schema"]}.reviews'
tgt = f'{cat}.{cfg["environment"]["silver_schema"]}.reviews'

# Read the Bronze reviews Delta table into a Spark DataFrame.
bronze = read_delta(spark, src)

# COMMAND ----------

# Build the cleaned Silver DataFrame by chaining transformations.
# .transform(...) lets us plug in reusable helper functions inside the chain.
clean = (
    bronze
    # Keep only the most recent row per review_id (newest ingest_ts wins) so duplicates from
    # re-ingested files don't inflate counts.
    .transform(lambda d: deduplicate(d, "review_id", order_col="ingest_ts"))
    # Drop rows with no review_id — the primary key must exist for a valid review.
    .transform(lambda d: drop_null_keys(d, "review_id"))
    # Cast the score to int; raw CSV values arrive as strings.
    .withColumn("review_score", F.col("review_score").cast("int"))
    # Standardize the score: nulls or out-of-range values (not 0-5) become 0 instead of
    # being dropped, so the review row still survives for other analysis.
    .withColumn("review_score",
                F.when(F.col("review_score").isNull(), F.lit(0))
                 .when((F.col("review_score") < 0) | (F.col("review_score") > 5), F.lit(0))
                 .otherwise(F.col("review_score")))
    # Parse the date/timestamp strings into real timestamp types using the source format.
    .withColumn("review_creation_date", F.to_timestamp("review_creation_date", "yyyy-MM-dd HH:mm:ss"))
    # answer_timestamp may legitimately be null (review not yet answered) — that's allowed.
    .withColumn("review_answer_timestamp", F.to_timestamp("review_answer_timestamp", "yyyy-MM-dd HH:mm:ss"))
    # Trim leading/trailing whitespace from all string columns (e.g. comment title/message).
    .transform(trim_strings)
    # Remove Bronze-only bookkeeping columns we don't carry into Silver.
    .drop("_rescued_data", "source_file")
)
# Append standard audit columns (e.g. processed timestamp) for lineage/traceability.
clean = add_audit_columns(clean)

# COMMAND ----------

# Data-quality gate: fail the notebook if any score still falls outside 0-5.
# Catching bad data here prevents it from polluting downstream Gold tables.
assert clean.filter((F.col("review_score") < 0) | (F.col("review_score") > 5)).count() == 0, "review_score out of range"
# Overwrite the Silver table (full refresh) with the cleaned data.
write_delta(clean, tgt, mode="overwrite")
# Attach a human-readable description to the table for documentation in the catalog.
spark.sql(f"COMMENT ON TABLE {tgt} IS 'Silver reviews: score defaulted/validated 0-5, dates cast, deduped.'")
# Quick sanity checks: total row count, then distribution of scores (catches data skew/errors).
print(f"Silver reviews rows: {clean.count():,}")
display(clean.groupBy("review_score").count().orderBy("review_score"))

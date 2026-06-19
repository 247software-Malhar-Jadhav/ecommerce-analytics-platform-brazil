# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold — dim_category (SCD-0 · Static)
# MAGIC Reference data that never changes historically (spec). Full overwrite each run.
# MAGIC Business key: `product_category_name` (Portuguese) | Surrogate: `category_sk`.

# COMMAND ----------

# MAGIC %run ../04_utils/common_functions

# COMMAND ----------

cfg = load_config()
cat = cfg["environment"]["catalog"]
silver = cfg["environment"]["silver_schema"]
gold = cfg["environment"]["gold_schema"]
tgt = f"{cat}.{gold}.dim_category"

# COMMAND ----------

dim = (
    read_delta(spark, f"{cat}.{silver}.category_translation")
    .select(
        # Surrogate key: deterministic hash of the business key (the category name).
        F.xxhash64(F.col("product_category_name")).alias("category_sk"),
        "product_category_name",            # business key (Portuguese)
        "product_category_name_english",    # readable English label for BI
    )
    .withColumn("updated_ts", F.current_timestamp())   # audit timestamp
)

# SCD-0 (static reference): just fully overwrite every run. No history, no MERGE,
# because categories are treated as fixed reference data that never changes.
write_delta(dim, tgt, mode="overwrite")
spark.sql(f"COMMENT ON TABLE {tgt} IS 'Gold dim_category (SCD-0 static reference).'")
print(f"dim_category rows: {dim.count():,}")
display(dim.limit(10))

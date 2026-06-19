# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold — dim_geolocation (optional, SCD-1)
# MAGIC One row per zip prefix with representative coordinates. Used to enrich city/state
# MAGIC level reporting (scenario #9: regional managers track city/state performance).

# COMMAND ----------

# MAGIC %run ../04_utils/common_functions

# COMMAND ----------

cfg = load_config()
cat = cfg["environment"]["catalog"]
silver = cfg["environment"]["silver_schema"]
gold = cfg["environment"]["gold_schema"]
tgt = f"{cat}.{gold}.dim_geolocation"

# COMMAND ----------

dim = (
    read_delta(spark, f"{cat}.{silver}.geolocation")
    .select(
        # Surrogate key: hash of the zip prefix (cast to string first so the hash
        # is computed over a consistent type). Business key = geolocation_zip_code_prefix.
        F.xxhash64(F.col("geolocation_zip_code_prefix").cast("string")).alias("geolocation_sk"),
        # Representative coordinates + city/state for the zip prefix.
        "geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng",
        "geolocation_city", "geolocation_state",
    )
    .withColumn("updated_ts", F.current_timestamp())   # audit timestamp
)

# SCD-1 via simple full overwrite: keep only the latest snapshot per zip prefix.
write_delta(dim, tgt, mode="overwrite")
spark.sql(f"COMMENT ON TABLE {tgt} IS 'Gold dim_geolocation (optional). 1 row per zip prefix.'")
print(f"dim_geolocation rows: {dim.count():,}")
display(dim.limit(5))

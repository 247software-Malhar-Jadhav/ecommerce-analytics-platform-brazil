# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — geolocation
# MAGIC * `geolocation_lat`/`geolocation_lng` → double, validate plausible Brazil bounds
# MAGIC * `geolocation_city` → lowercase+trim (normalize), `geolocation_state` → uppercase
# MAGIC * `geolocation_zip_code_prefix` → int
# MAGIC * Collapse to one representative (avg lat/lng) row per zip prefix for the optional dim
# MAGIC * overwrite

# COMMAND ----------

# MAGIC %run ../04_utils/common_functions

# COMMAND ----------

cfg = load_config()
cat = cfg["environment"]["catalog"]
src = f'{cat}.{cfg["environment"]["bronze_schema"]}.geolocation'
tgt = f'{cat}.{cfg["environment"]["silver_schema"]}.geolocation'

bronze = read_delta(spark, src)

# COMMAND ----------

typed = (
    bronze
    .withColumn("geolocation_zip_code_prefix",
                F.regexp_replace(F.col("geolocation_zip_code_prefix"), r"\s+", "").cast("int"))
    .withColumn("geolocation_lat", F.col("geolocation_lat").cast("double"))
    .withColumn("geolocation_lng", F.col("geolocation_lng").cast("double"))
    .withColumn("geolocation_city", F.lower(F.trim(F.col("geolocation_city"))))
    .withColumn("geolocation_state", F.upper(F.trim(F.col("geolocation_state"))))
    .filter(F.col("geolocation_zip_code_prefix").isNotNull())
    # Brazil lat/lng bounds — drop clearly corrupt coordinates
    .filter((F.col("geolocation_lat").between(-35.0, 6.0)) & (F.col("geolocation_lng").between(-75.0, -33.0)))
)

# One representative location per zip prefix (most frequent city/state + mean coords)
clean = (
    typed.groupBy("geolocation_zip_code_prefix")
    .agg(
        F.round(F.avg("geolocation_lat"), 6).alias("geolocation_lat"),
        F.round(F.avg("geolocation_lng"), 6).alias("geolocation_lng"),
        F.first("geolocation_city", ignorenulls=True).alias("geolocation_city"),
        F.first("geolocation_state", ignorenulls=True).alias("geolocation_state"),
    )
)
clean = add_audit_columns(clean)

# COMMAND ----------

write_delta(clean, tgt, mode="overwrite")
spark.sql(f"COMMENT ON TABLE {tgt} IS 'Silver geolocation: coords validated, normalized, 1 representative row per zip prefix.'")
print(f"Silver geolocation rows: {clean.count():,}")
display(clean.limit(5))

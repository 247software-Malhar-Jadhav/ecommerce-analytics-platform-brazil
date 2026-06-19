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

# Load project settings and build fully-qualified table names (catalog.schema.table).
cfg = load_config()
cat = cfg["environment"]["catalog"]
src = f'{cat}.{cfg["environment"]["bronze_schema"]}.geolocation'    # Bronze input
tgt = f'{cat}.{cfg["environment"]["silver_schema"]}.geolocation'   # Silver output

# Read the Bronze geolocation Delta table.
bronze = read_delta(spark, src)

# COMMAND ----------

# Step 1 — cast columns to proper types, normalize text, and filter out bad coordinates.
typed = (
    bronze
    # Strip whitespace from the zip prefix and cast to int (the grouping key below).
    .withColumn("geolocation_zip_code_prefix",
                F.regexp_replace(F.col("geolocation_zip_code_prefix"), r"\s+", "").cast("int"))
    # Latitude/longitude must be numeric doubles for averaging.
    .withColumn("geolocation_lat", F.col("geolocation_lat").cast("double"))
    .withColumn("geolocation_lng", F.col("geolocation_lng").cast("double"))
    # Normalize text: city lowercase, state uppercase.
    .withColumn("geolocation_city", F.lower(F.trim(F.col("geolocation_city"))))
    .withColumn("geolocation_state", F.upper(F.trim(F.col("geolocation_state"))))
    # Drop rows without a zip prefix — we can't group them.
    .filter(F.col("geolocation_zip_code_prefix").isNotNull())
    # Brazil lat/lng bounds — drop clearly corrupt coordinates
    # (points outside Brazil's box are bad data that would skew the averaged location).
    .filter((F.col("geolocation_lat").between(-35.0, 6.0)) & (F.col("geolocation_lng").between(-75.0, -33.0)))
)

# Step 2 — collapse many raw points per zip prefix into ONE representative row.
# The raw data has multiple coordinate readings per zip; we average them and pick a
# city/state so each zip prefix maps to a single clean location for downstream joins.
clean = (
    typed.groupBy("geolocation_zip_code_prefix")
    .agg(
        # Mean coordinates, rounded to 6 decimals (~0.1 m precision — plenty for a region).
        F.round(F.avg("geolocation_lat"), 6).alias("geolocation_lat"),
        F.round(F.avg("geolocation_lng"), 6).alias("geolocation_lng"),
        # Take the first non-null city/state seen within each zip-prefix group.
        F.first("geolocation_city", ignorenulls=True).alias("geolocation_city"),
        F.first("geolocation_state", ignorenulls=True).alias("geolocation_state"),
    )
)
# Add standard audit columns for lineage.
clean = add_audit_columns(clean)

# COMMAND ----------

# Overwrite the Silver geolocation table (full refresh) and document it in the catalog.
write_delta(clean, tgt, mode="overwrite")
spark.sql(f"COMMENT ON TABLE {tgt} IS 'Silver geolocation: coords validated, normalized, 1 representative row per zip prefix.'")
# Sanity check: row count and a small preview.
print(f"Silver geolocation rows: {clean.count():,}")
display(clean.limit(5))

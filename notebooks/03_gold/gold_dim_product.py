# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold — dim_product (SCD-1)
# MAGIC Corrections overwrite (spec). Joins the English category name from
# MAGIC `silver.category_translation` so BI users get readable categories.
# MAGIC
# MAGIC * Business key: `product_id`  | Surrogate: `product_sk` = xxhash64(product_id)

# COMMAND ----------

# MAGIC %run ../04_utils/common_functions

# COMMAND ----------

# DeltaTable gives us the MERGE (upsert) API used below for SCD-1.
from delta.tables import DeltaTable

# Load config so catalog/schema/table names are not hard-coded.
cfg = load_config()
cat = cfg["environment"]["catalog"]
silver = cfg["environment"]["silver_schema"]
gold = cfg["environment"]["gold_schema"]
tgt = f"{cat}.{gold}.dim_product"

# Two Silver inputs: the raw products, and the Portuguese->English category lookup.
products = read_delta(spark, f"{cat}.{silver}.products")
translation = read_delta(spark, f"{cat}.{silver}.category_translation")

# COMMAND ----------

src_df = (
    products.alias("p")
    # LEFT join the translation so every product survives even if it has no
    # category match (a product with no English name should not be dropped).
    .join(translation.alias("c"), F.col("p.product_category_name") == F.col("c.product_category_name"), "left")
    .select(
        # Surrogate key: deterministic hash of the business key (product_id).
        # Same product_id -> same product_sk every run, so joins stay stable.
        F.xxhash64(F.col("p.product_id")).alias("product_sk"),
        F.col("p.product_id"),                                   # business key
        F.col("p.product_category_name"),                       # original Portuguese name
        # English category for BI users; fall back to "unknown" when no translation matched.
        F.coalesce(F.col("c.product_category_name_english"), F.lit("unknown")).alias("product_category_name_english"),
        # Descriptive measures/attributes of the product.
        "product_name_length", "product_description_length", "product_photos_qty",
        "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm",
    )
    .withColumn("updated_ts", F.current_timestamp())   # audit timestamp
)

# COMMAND ----------

# First run: create the dimension from the full snapshot.
if not table_exists(spark, tgt):
    write_delta(src_df, tgt, mode="overwrite")
    print(f"Created {tgt} with {src_df.count():,} rows")
else:
    # Later runs: SCD-1 upsert on the business key (product_id).
    dim = DeltaTable.forName(spark, tgt)
    (
        dim.alias("t")
        .merge(src_df.alias("s"), "t.product_id = s.product_id")
        .whenMatchedUpdateAll()      # SCD-1: overwrite changed attributes in place (no history)
        .whenNotMatchedInsertAll()   # insert products not yet in the dimension
        .execute()
    )
    print(f"MERGE complete into {tgt}")

# COMMAND ----------

spark.sql(f"COMMENT ON TABLE {tgt} IS 'Gold dim_product (SCD-1) with English category join.'")
display(spark.table(tgt).limit(5))

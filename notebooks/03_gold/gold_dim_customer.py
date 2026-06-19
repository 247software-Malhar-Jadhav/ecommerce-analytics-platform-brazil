# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold — dim_customer (SCD-1)
# MAGIC No history needed (spec): corrections simply overwrite. We MERGE the latest Silver
# MAGIC snapshot into the dimension so the surrogate key (`customer_sk`) stays **stable** across
# MAGIC daily runs while descriptive attributes are overwritten in place.
# MAGIC
# MAGIC * Business key: `customer_id`
# MAGIC * Surrogate key: `customer_sk` = xxhash64(customer_id)  (deterministic, stable)

# COMMAND ----------

# MAGIC %run ../04_utils/common_functions

# COMMAND ----------

from delta.tables import DeltaTable

cfg = load_config()
cat = cfg["environment"]["catalog"]
silver = cfg["environment"]["silver_schema"]
gold = cfg["environment"]["gold_schema"]
src = f"{cat}.{silver}.customers"
tgt = f"{cat}.{gold}.dim_customer"

# COMMAND ----------

src_df = (
    read_delta(spark, src)
    .select(
        F.xxhash64(F.col("customer_id")).alias("customer_sk"),
        "customer_id", "customer_unique_id", "customer_zip_code_prefix",
        "customer_city", "customer_state",
    )
    .withColumn("updated_ts", F.current_timestamp())
)

# COMMAND ----------

if not table_exists(spark, tgt):
    write_delta(src_df, tgt, mode="overwrite")
    print(f"Created {tgt} with {src_df.count():,} rows")
else:
    dim = DeltaTable.forName(spark, tgt)
    (
        dim.alias("t")
        .merge(src_df.alias("s"), "t.customer_id = s.customer_id")
        .whenMatchedUpdateAll()      # SCD-1 overwrite of changed attributes
        .whenNotMatchedInsertAll()   # new customers
        .execute()
    )
    print(f"MERGE complete into {tgt}")

# COMMAND ----------

assert spark.table(tgt).count() == spark.table(tgt).select("customer_id").distinct().count(), "dup business key"
spark.sql(f"COMMENT ON TABLE {tgt} IS 'Gold dim_customer (SCD-1). Stable customer_sk, attributes overwritten on change.'")
display(spark.table(tgt).limit(5))

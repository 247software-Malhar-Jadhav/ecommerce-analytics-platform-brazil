# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold — dim_seller (SCD-2 · History Tracking)
# MAGIC Location history matters (spec scenario #4: seller moved → delivery performance changed).
# MAGIC We track changes to `seller_city` / `seller_state` over time.
# MAGIC
# MAGIC SCD-2 columns added:
# MAGIC * `seller_sk`      — surrogate key, unique per version (xxhash64 of business key + effective_from)
# MAGIC * `effective_from` — when this version became active
# MAGIC * `effective_to`   — when it was superseded (NULL / high-date for current)
# MAGIC * `is_current`     — boolean flag for the live version
# MAGIC
# MAGIC Algorithm (Delta two-step MERGE):
# MAGIC 1. Detect rows whose tracked attributes changed vs the current dim version.
# MAGIC 2. **Expire** the old current row (set effective_to, is_current=false).
# MAGIC 3. **Insert** the new version as current.

# COMMAND ----------

# MAGIC %run ../04_utils/common_functions

# COMMAND ----------

from delta.tables import DeltaTable

cfg = load_config()
cat = cfg["environment"]["catalog"]
silver = cfg["environment"]["silver_schema"]
gold = cfg["environment"]["gold_schema"]
tgt = f"{cat}.{gold}.dim_seller"

HIGH_DATE = "9999-12-31 00:00:00"
TRACKED = ["seller_city", "seller_state"]  # changes here create a new version

# COMMAND ----------

src = (
    read_delta(spark, f"{cat}.{silver}.sellers")
    .select("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state")
    .withColumn("effective_from", F.current_timestamp())
    .withColumn("effective_to", F.to_timestamp(F.lit(HIGH_DATE)))
    .withColumn("is_current", F.lit(True))
    .withColumn("seller_sk", F.xxhash64(F.concat_ws("|", F.col("seller_id"), F.col("effective_from").cast("string"))))
)

# COMMAND ----------

# MAGIC %md ### First load — create the dimension as-is

# COMMAND ----------

if not table_exists(spark, tgt):
    write_delta(src, tgt, mode="overwrite")
    print(f"Created {tgt} with {src.count():,} current rows")
    dbutils.notebook.exit("created")

# COMMAND ----------

# MAGIC %md ### Incremental SCD-2 — detect changes, expire, insert

# COMMAND ----------

dim = DeltaTable.forName(spark, tgt)
current = dim.toDF().filter(F.col("is_current") == True)  # noqa: E712

change_cond = " OR ".join([f"t.{c} <> s.{c}" for c in TRACKED])

# Rows that are new or changed vs the current dimension version
joined = (
    src.alias("s")
    .join(current.alias("t"), "seller_id", "left")
    .filter(F.col("t.seller_id").isNull() | F.expr(change_cond))
    .select("s.*")
)

changed_or_new = joined.count()
print(f"Sellers new-or-changed this run: {changed_or_new:,}")

# Step 1: expire current rows whose tracked attributes changed.
(
    dim.alias("t")
    .merge(
        joined.alias("s"),
        "t.seller_id = s.seller_id AND t.is_current = true",
    )
    .whenMatchedUpdate(
        condition=F.expr(change_cond),
        set={
            "is_current": F.lit(False),
            "effective_to": F.current_timestamp(),
        },
    )
    .execute()
)

# Step 2: insert the new current versions (both brand-new sellers and changed ones).
write_delta(joined, tgt, mode="append")

# COMMAND ----------

# DQ: exactly one current row per seller_id
dup_current = (
    spark.table(tgt).filter(F.col("is_current") == True)  # noqa: E712
    .groupBy("seller_id").count().filter(F.col("count") > 1).count()
)
assert dup_current == 0, f"{dup_current} sellers have >1 current row — SCD-2 invariant broken"
spark.sql(f"COMMENT ON TABLE {tgt} IS 'Gold dim_seller (SCD-2). effective_from/to + is_current track location history.'")
display(spark.table(tgt).orderBy("seller_id", "effective_from").limit(10))

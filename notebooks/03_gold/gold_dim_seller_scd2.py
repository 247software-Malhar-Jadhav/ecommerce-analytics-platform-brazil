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

# DeltaTable gives us the MERGE API used to "expire" old versions below.
from delta.tables import DeltaTable

cfg = load_config()
cat = cfg["environment"]["catalog"]
silver = cfg["environment"]["silver_schema"]
gold = cfg["environment"]["gold_schema"]
tgt = f"{cat}.{gold}.dim_seller"

# Sentinel "end of time" timestamp used for the currently-active version's effective_to.
# Using a far-future date (instead of NULL) makes BETWEEN-style date filters simpler.
HIGH_DATE = "9999-12-31 00:00:00"
# Only changes to these columns trigger a new SCD-2 version. Other columns changing
# would just be ignored here (we are tracking location history specifically).
TRACKED = ["seller_city", "seller_state"]  # changes here create a new version

# COMMAND ----------

# Build the incoming snapshot, stamped as a brand-new "current" version.
src = (
    read_delta(spark, f"{cat}.{silver}.sellers")
    .select("seller_id", "seller_zip_code_prefix", "seller_city", "seller_state")
    # This version becomes active now...
    .withColumn("effective_from", F.current_timestamp())
    # ...and has no end yet, so set effective_to to the far-future sentinel.
    .withColumn("effective_to", F.to_timestamp(F.lit(HIGH_DATE)))
    # Flag marking this as the live/active version.
    .withColumn("is_current", F.lit(True))
    # SCD-2 surrogate key must be unique PER VERSION (not per seller), so we hash
    # the business key combined with effective_from. The same seller will get a
    # different seller_sk each time its location changes -> one SK per history row.
    .withColumn("seller_sk", F.xxhash64(F.concat_ws("|", F.col("seller_id"), F.col("effective_from").cast("string"))))
)

# COMMAND ----------

# MAGIC %md ### First load — create the dimension as-is

# COMMAND ----------

# First load: no dimension yet, so every seller's first version is simply written
# out as current. We then exit early — there is nothing to compare/expire on run #1.
if not table_exists(spark, tgt):
    write_delta(src, tgt, mode="overwrite")
    print(f"Created {tgt} with {src.count():,} current rows")
    dbutils.notebook.exit("created")

# COMMAND ----------

# MAGIC %md ### Incremental SCD-2 — detect changes, expire, insert

# COMMAND ----------

# Load the existing dimension and keep only the live versions to compare against.
dim = DeltaTable.forName(spark, tgt)
current = dim.toDF().filter(F.col("is_current") == True)  # noqa: E712

# Build a SQL boolean like "t.seller_city <> s.seller_city OR t.seller_state <> s.seller_state".
# It is TRUE when any tracked attribute differs between the stored current row (t)
# and the incoming snapshot (s) -> i.e. the seller's location changed.
change_cond = " OR ".join([f"t.{c} <> s.{c}" for c in TRACKED])

# Find the sellers we actually need to act on this run:
#   - brand-new sellers (no current row exists -> t.seller_id IS NULL), OR
#   - existing sellers whose tracked attributes changed (change_cond is true).
# Unchanged sellers are intentionally filtered out so no spurious versions are created.
joined = (
    src.alias("s")
    .join(current.alias("t"), "seller_id", "left")
    .filter(F.col("t.seller_id").isNull() | F.expr(change_cond))
    .select("s.*")   # keep only the incoming columns -> these are the rows to insert
)

changed_or_new = joined.count()
print(f"Sellers new-or-changed this run: {changed_or_new:,}")

# Step 1 (EXPIRE): close out the old current row for sellers whose location changed.
# We MERGE the changed/new rows against the dim, matching on the live version only.
(
    dim.alias("t")
    .merge(
        joined.alias("s"),
        # Match each incoming seller to its single current row in the dim.
        "t.seller_id = s.seller_id AND t.is_current = true",
    )
    # Only when a tracked attribute actually differs, retire the old row:
    # flip is_current to false and stamp effective_to with "now" (its end of life).
    # (Brand-new sellers have no matching current row, so they are untouched here.)
    .whenMatchedUpdate(
        condition=F.expr(change_cond),
        set={
            "is_current": F.lit(False),
            "effective_to": F.current_timestamp(),
        },
    )
    .execute()
)

# Step 2 (INSERT): append the new current versions. This covers both brand-new
# sellers and the new version of sellers we just expired in Step 1. Together the
# two steps implement the SCD-2 "expire old + insert new" pattern, preserving history.
write_delta(joined, tgt, mode="append")

# COMMAND ----------

# Data-quality check: the core SCD-2 invariant is that each seller has exactly ONE
# current (is_current=true) row. Count any seller with more than one -> must be zero.
dup_current = (
    spark.table(tgt).filter(F.col("is_current") == True)  # noqa: E712
    .groupBy("seller_id").count().filter(F.col("count") > 1).count()
)
assert dup_current == 0, f"{dup_current} sellers have >1 current row — SCD-2 invariant broken"
spark.sql(f"COMMENT ON TABLE {tgt} IS 'Gold dim_seller (SCD-2). effective_from/to + is_current track location history.'")
display(spark.table(tgt).orderBy("seller_id", "effective_from").limit(10))

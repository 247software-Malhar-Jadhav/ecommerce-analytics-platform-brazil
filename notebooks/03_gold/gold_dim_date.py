# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold — dim_date (Derived calendar)
# MAGIC Generated, not sourced. Covers the full order date range plus headroom so the fact's
# MAGIC `date_id` (yyyyMMdd int) always resolves. Supports daily/MoM/seasonal analysis.

# COMMAND ----------

# MAGIC %run ../04_utils/common_functions

# COMMAND ----------

cfg = load_config()
cat = cfg["environment"]["catalog"]
gold = cfg["environment"]["gold_schema"]
tgt = f"{cat}.{gold}.dim_date"

START_DATE = "2016-01-01"   # Olist data begins late 2016
END_DATE = "2030-12-31"     # headroom for future loads

# COMMAND ----------

# Generate one row per calendar day between START_DATE and END_DATE.
# sequence(...) builds an array of every date in the range; explode() turns that
# single array into one row per element -> a complete, gap-free calendar.
dim = (
    spark.sql(f"SELECT explode(sequence(to_date('{START_DATE}'), to_date('{END_DATE}'), interval 1 day)) AS date")
    # Surrogate key: the date as an integer yyyyMMdd (e.g. 2017-05-09 -> 20170509).
    # The fact table stores this same int as its date_id, so they join directly.
    .withColumn("date_id", F.date_format("date", "yyyyMMdd").cast("int"))
    # Pre-computed calendar attributes so BI tools can slice without date math.
    .withColumn("year", F.year("date"))
    .withColumn("quarter", F.quarter("date"))
    .withColumn("month", F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMMM"))       # e.g. "January"
    .withColumn("day", F.dayofmonth("date"))
    .withColumn("day_of_week", F.dayofweek("date"))                # 1=Sunday ... 7=Saturday
    .withColumn("day_name", F.date_format("date", "EEEE"))         # e.g. "Monday"
    .withColumn("week_of_year", F.weekofyear("date"))
    # Weekend = Sunday(1) or Saturday(7) under Spark's dayofweek numbering.
    .withColumn("is_weekend", F.dayofweek("date").isin(1, 7))
    .withColumn("year_month", F.date_format("date", "yyyy-MM"))    # handy "2017-05" grouping key
)

# Overwrite the whole calendar each run (it is fully derived, so it is cheap and
# deterministic to rebuild). Select an explicit column order for a tidy dimension.
write_delta(dim.select(
    "date_id", "date", "year", "quarter", "month", "month_name", "day",
    "day_of_week", "day_name", "week_of_year", "is_weekend", "year_month",
), tgt, mode="overwrite")

spark.sql(f"COMMENT ON TABLE {tgt} IS 'Gold dim_date derived calendar, int yyyyMMdd surrogate key.'")
print(f"dim_date rows: {spark.table(tgt).count():,}")
display(spark.table(tgt).limit(5))

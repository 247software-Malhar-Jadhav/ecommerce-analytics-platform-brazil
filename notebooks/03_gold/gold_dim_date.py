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

dim = (
    spark.sql(f"SELECT explode(sequence(to_date('{START_DATE}'), to_date('{END_DATE}'), interval 1 day)) AS date")
    .withColumn("date_id", F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("date"))
    .withColumn("quarter", F.quarter("date"))
    .withColumn("month", F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("day", F.dayofmonth("date"))
    .withColumn("day_of_week", F.dayofweek("date"))
    .withColumn("day_name", F.date_format("date", "EEEE"))
    .withColumn("week_of_year", F.weekofyear("date"))
    .withColumn("is_weekend", F.dayofweek("date").isin(1, 7))
    .withColumn("year_month", F.date_format("date", "yyyy-MM"))
)

write_delta(dim.select(
    "date_id", "date", "year", "quarter", "month", "month_name", "day",
    "day_of_week", "day_name", "week_of_year", "is_weekend", "year_month",
), tgt, mode="overwrite")

spark.sql(f"COMMENT ON TABLE {tgt} IS 'Gold dim_date derived calendar, int yyyyMMdd surrogate key.'")
print(f"dim_date rows: {spark.table(tgt).count():,}")
display(spark.table(tgt).limit(5))

# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold — fact_orders (Star schema fact)
# MAGIC
# MAGIC **Grain:** one row per *order line item* (`order_id` + `order_item_id`). This is the
# MAGIC finest grain that still carries product & seller, so every dimension joins cleanly.
# MAGIC
# MAGIC **Sources joined (Silver):** order_items ⋈ orders ⋈ products ⋈ sellers(current) ⋈
# MAGIC payments(order-level) ⋈ reviews(order-level).
# MAGIC
# MAGIC **Measures:** `price`, `freight_value` (per item), `payment_value` (order total
# MAGIC *allocated* to each item by price-share so revenue is never double-counted),
# MAGIC `review_score`, `delivery_days` (delivered − purchased).
# MAGIC
# MAGIC **Load:** append-only — anti-join on the natural key drops rows already in the fact, so
# MAGIC re-runs are idempotent. Partitioned by `order_year_month` for fast time-range queries.

# COMMAND ----------

# MAGIC %run ../04_utils/common_functions

# COMMAND ----------

cfg = load_config()
cat = cfg["environment"]["catalog"]
silver = cfg["environment"]["silver_schema"]
gold = cfg["environment"]["gold_schema"]
tgt = f"{cat}.{gold}.fact_orders"

items = read_delta(spark, f"{cat}.{silver}.order_items")
orders = read_delta(spark, f"{cat}.{silver}.orders")
payments = read_delta(spark, f"{cat}.{silver}.payments")
reviews = read_delta(spark, f"{cat}.{silver}.reviews")
sellers = read_delta(spark, f"{cat}.{gold}.dim_seller").filter(F.col("is_current") == True)  # noqa: E712

# COMMAND ----------

# MAGIC %md ### Pre-aggregate order-level sources to avoid fan-out

# COMMAND ----------

# Payments are per (order, payment_sequential) -> roll up to order grain.
pay_order = payments.groupBy("order_id").agg(
    F.sum("payment_value").alias("order_payment_value"),
    F.max("payment_installments").alias("payment_installments"),
    F.first("payment_type", ignorenulls=True).alias("payment_type"),
)

# Multiple reviews per order are possible -> take the latest review's score.
rev_order = (
    deduplicate(reviews, "order_id", order_col="review_creation_date")
    .select("order_id", "review_score")
)

# Price share per item within its order, for proportional payment allocation.
from pyspark.sql.window import Window
w_order = Window.partitionBy("order_id")
items_share = items.withColumn(
    "price_share",
    F.when(F.sum("price").over(w_order) > 0, F.col("price") / F.sum("price").over(w_order)).otherwise(F.lit(0.0)),
)

# COMMAND ----------

# MAGIC %md ### Build the fact

# COMMAND ----------

fact = (
    items_share.alias("i")
    .join(orders.alias("o"), "order_id", "inner")
    .join(pay_order.alias("p"), "order_id", "left")
    .join(rev_order.alias("r"), "order_id", "left")
    .join(sellers.alias("s"), "seller_id", "left")
    .select(
        # natural key (for idempotent append)
        F.col("i.order_id"),
        F.col("i.order_item_id"),
        # foreign / surrogate keys -> star schema
        F.col("i.product_id"),
        F.col("o.customer_id"),
        F.col("i.seller_id"),
        F.col("s.seller_sk").alias("seller_sk"),
        F.xxhash64(F.col("o.customer_id")).alias("customer_sk"),
        F.xxhash64(F.col("i.product_id")).alias("product_sk"),
        F.date_format(F.col("o.order_purchase_timestamp"), "yyyyMMdd").cast("int").alias("date_id"),
        # degenerate dimension
        F.col("o.order_status"),
        # measures
        F.col("i.price").alias("price"),
        F.col("i.freight_value").alias("freight_value"),
        F.round(F.coalesce(F.col("p.order_payment_value"), F.lit(0.0)) * F.col("i.price_share"), 2).alias("payment_value"),
        F.coalesce(F.col("p.payment_installments"), F.lit(0)).alias("payment_installments"),
        F.col("p.payment_type"),
        F.coalesce(F.col("r.review_score"), F.lit(0)).alias("review_score"),
        F.datediff(F.col("o.order_delivered_customer_date"), F.col("o.order_purchase_timestamp")).alias("delivery_days"),
        # partition column
        F.date_format(F.col("o.order_purchase_timestamp"), "yyyy-MM").alias("order_year_month"),
    )
    .withColumn("updated_ts", F.current_timestamp())
)

# COMMAND ----------

# MAGIC %md ### Idempotent append (only new natural keys)

# COMMAND ----------

if table_exists(spark, tgt):
    existing_keys = spark.table(tgt).select("order_id", "order_item_id").distinct()
    fact_new = fact.join(existing_keys, ["order_id", "order_item_id"], "left_anti")
else:
    fact_new = fact

new_rows = fact_new.count()
print(f"New fact rows to append: {new_rows:,}")

if new_rows > 0:
    write_delta(fact_new, tgt, mode="append", partition_by="order_year_month")

# COMMAND ----------

# MAGIC %md ### Optimize & validate

# COMMAND ----------

spark.sql(f"OPTIMIZE {tgt}")
spark.sql(f"COMMENT ON TABLE {tgt} IS 'Gold fact_orders. Grain=order line item. Payment allocated by price-share. Append-only, partitioned by order_year_month.'")

total = spark.table(tgt).count()
distinct_keys = spark.table(tgt).select("order_id", "order_item_id").distinct().count()
assert total == distinct_keys, f"Duplicate grain in fact_orders: {total} rows vs {distinct_keys} keys"
print(f"fact_orders total rows: {total:,}")
display(spark.table(tgt).limit(5))

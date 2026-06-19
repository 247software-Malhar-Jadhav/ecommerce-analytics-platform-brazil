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

# Load every source the fact needs. Note the grains differ:
items = read_delta(spark, f"{cat}.{silver}.order_items")     # one row per order LINE ITEM
orders = read_delta(spark, f"{cat}.{silver}.orders")          # one row per order
payments = read_delta(spark, f"{cat}.{silver}.payments")      # one row per payment line
reviews = read_delta(spark, f"{cat}.{silver}.reviews")        # one (or more) rows per order
# Sellers come from the SCD-2 dim; keep only CURRENT versions so each seller_id
# resolves to exactly one seller_sk (joining all versions would fan out rows).
sellers = read_delta(spark, f"{cat}.{gold}.dim_seller").filter(F.col("is_current") == True)  # noqa: E712

# COMMAND ----------

# MAGIC %md ### Pre-aggregate order-level sources to avoid fan-out

# COMMAND ----------

# An order can have several payment lines. If we joined them straight to items
# we'd multiply (fan out) the line-item rows. So first roll payments up to ONE row
# per order: total paid, max installments, and a representative payment type.
pay_order = payments.groupBy("order_id").agg(
    F.sum("payment_value").alias("order_payment_value"),
    F.max("payment_installments").alias("payment_installments"),
    F.first("payment_type", ignorenulls=True).alias("payment_type"),
)

# Same fan-out risk with reviews: an order may have multiple reviews. Deduplicate
# down to one row per order, keeping the latest review (by review_creation_date).
rev_order = (
    deduplicate(reviews, "order_id", order_col="review_creation_date")
    .select("order_id", "review_score")
)

# PRICE-SHARE allocation: the order's single payment total must be split across its
# line items without double-counting revenue. Each item's share = its price divided
# by the total price of all items in the same order (a window over order_id).
# Guard against divide-by-zero: if the order total price is 0, the share is 0.0.
from pyspark.sql.window import Window
w_order = Window.partitionBy("order_id")   # window = all items belonging to one order
items_share = items.withColumn(
    "price_share",
    F.when(F.sum("price").over(w_order) > 0, F.col("price") / F.sum("price").over(w_order)).otherwise(F.lit(0.0)),
)

# COMMAND ----------

# MAGIC %md ### Build the fact

# COMMAND ----------

# Assemble the fact at the line-item grain. Start from items (the grain driver) and
# enrich with the pre-aggregated order-level sources so no join multiplies rows.
fact = (
    items_share.alias("i")
    # INNER join orders: a line item with no parent order is invalid -> drop it.
    .join(orders.alias("o"), "order_id", "inner")
    # LEFT joins for optional context: an order may lack payments/reviews/seller,
    # but we must still keep the line-item row (measures will be coalesced below).
    .join(pay_order.alias("p"), "order_id", "left")
    .join(rev_order.alias("r"), "order_id", "left")
    .join(sellers.alias("s"), "seller_id", "left")
    .select(
        # Natural (business) key = order_id + order_item_id. This IS the fact grain
        # and is used later for the idempotent anti-join on re-runs.
        F.col("i.order_id"),
        F.col("i.order_item_id"),
        # Foreign keys into the dimensions (both natural ids and surrogate keys).
        F.col("i.product_id"),
        F.col("o.customer_id"),
        F.col("i.seller_id"),
        F.col("s.seller_sk").alias("seller_sk"),   # SCD-2 SK of the seller's current version
        # Recompute customer/product SKs with the SAME xxhash64 formula the dims use,
        # so these foreign keys match dim_customer.customer_sk / dim_product.product_sk.
        F.xxhash64(F.col("o.customer_id")).alias("customer_sk"),
        F.xxhash64(F.col("i.product_id")).alias("product_sk"),
        # date_id as int yyyyMMdd -> joins to dim_date.date_id.
        F.date_format(F.col("o.order_purchase_timestamp"), "yyyyMMdd").cast("int").alias("date_id"),
        # Degenerate dimension: an attribute stored on the fact itself (no dim table).
        F.col("o.order_status"),
        # Measures (the numbers analysts aggregate):
        F.col("i.price").alias("price"),                   # per-item product price
        F.col("i.freight_value").alias("freight_value"),   # per-item shipping cost
        # Allocate the order's total payment to THIS item by its price share, then
        # round to cents. This spreads one payment across items without double-counting.
        F.round(F.coalesce(F.col("p.order_payment_value"), F.lit(0.0)) * F.col("i.price_share"), 2).alias("payment_value"),
        # Coalesce optional measures to sensible defaults when the left join missed.
        F.coalesce(F.col("p.payment_installments"), F.lit(0)).alias("payment_installments"),
        F.col("p.payment_type"),
        F.coalesce(F.col("r.review_score"), F.lit(0)).alias("review_score"),
        # delivery_days = delivered date minus purchase date (a lead-time measure).
        F.datediff(F.col("o.order_delivered_customer_date"), F.col("o.order_purchase_timestamp")).alias("delivery_days"),
        # Partition column ("2017-05"): physically groups data by month for fast,
        # pruned time-range queries.
        F.date_format(F.col("o.order_purchase_timestamp"), "yyyy-MM").alias("order_year_month"),
    )
    .withColumn("updated_ts", F.current_timestamp())   # audit timestamp
)

# COMMAND ----------

# MAGIC %md ### Idempotent append (only new natural keys)

# COMMAND ----------

# Idempotency: only append line items not already present. A LEFT ANTI join keeps
# rows in `fact` whose (order_id, order_item_id) key is NOT in the existing table,
# so re-running the notebook never creates duplicates.
if table_exists(spark, tgt):
    existing_keys = spark.table(tgt).select("order_id", "order_item_id").distinct()
    fact_new = fact.join(existing_keys, ["order_id", "order_item_id"], "left_anti")
else:
    # First run: the table does not exist yet, so every built row is new.
    fact_new = fact

new_rows = fact_new.count()
print(f"New fact rows to append: {new_rows:,}")

# Append-only load, partitioned by month. Skip the write entirely if nothing is new.
if new_rows > 0:
    write_delta(fact_new, tgt, mode="append", partition_by="order_year_month")

# COMMAND ----------

# MAGIC %md ### Optimize & validate

# COMMAND ----------

# OPTIMIZE compacts the many small append files into larger ones for faster reads.
spark.sql(f"OPTIMIZE {tgt}")
spark.sql(f"COMMENT ON TABLE {tgt} IS 'Gold fact_orders. Grain=order line item. Payment allocated by price-share. Append-only, partitioned by order_year_month.'")

# Grain check: total rows must equal the number of distinct natural keys. If they
# differ, the fact has duplicate line items and the grain is broken.
total = spark.table(tgt).count()
distinct_keys = spark.table(tgt).select("order_id", "order_item_id").distinct().count()
assert total == distinct_keys, f"Duplicate grain in fact_orders: {total} rows vs {distinct_keys} keys"
print(f"fact_orders total rows: {total:,}")
display(spark.table(tgt).limit(5))

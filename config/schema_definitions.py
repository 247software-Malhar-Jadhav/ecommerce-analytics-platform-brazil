# Databricks notebook source
# MAGIC %md
# MAGIC # Source Schema Definitions
# MAGIC
# MAGIC Explicit `StructType` schemas for every Olist source file. The Bronze layer reads CSVs
# MAGIC with these schemas (instead of `inferSchema`) so that:
# MAGIC   * ingestion is deterministic and fast (no extra scan pass),
# MAGIC   * malformed rows are captured in a `_rescued_data` column instead of silently corrupting types,
# MAGIC   * the contract between source systems and Bronze is version-controlled in git.
# MAGIC
# MAGIC NOTE: in Bronze we deliberately read everything as the *closest raw type*. Heavy casting,
# MAGIC `$`-symbol stripping and special-character cleanup happen in Silver — never in Bronze.

# COMMAND ----------

# Building blocks for declaring a schema by hand:
#   StructType  = the whole table schema (a list of columns)
#   StructField = one column: (name, data type, nullable?)
#   StringType / IntegerType / ... = the column data types
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType,
)

# ---------------------------------------------------------------------------
# Each schema mirrors the raw CSV header exactly. We keep numeric/money columns
# as StringType in Bronze when the raw file may contain "$" or junk characters,
# so that Bronze never drops a row it cannot cast. Silver does the real casting.
# ---------------------------------------------------------------------------

# One row per customer record on an order (Olist links orders to a customer_id).
CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_unique_id", StringType(), True),
    StructField("customer_zip_code_prefix", StringType(), True),  # cast -> int in Silver (strip spaces)
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True),
])

# One row per order, including its status and the various delivery lifecycle timestamps.
ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", StringType(), True),     # cast -> timestamp in Silver
    StructField("order_approved_at", StringType(), True),
    StructField("order_delivered_carrier_date", StringType(), True),
    StructField("order_delivered_customer_date", StringType(), True),
    StructField("order_estimated_delivery_date", StringType(), True),
])

# One row per line item within an order (an order can contain several products).
ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_item_id", StringType(), True),                # cast -> int in Silver
    StructField("product_id", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("shipping_limit_date", StringType(), True),          # cast -> timestamp in Silver
    StructField("price", StringType(), True),                        # strip "$" -> double in Silver
    StructField("freight_value", StringType(), True),                # strip "$" -> double in Silver
])

# One row per payment on an order (an order may be paid in multiple sequential parts).
PAYMENTS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_sequential", StringType(), True),           # cast -> int in Silver
    StructField("payment_type", StringType(), True),                 # strip special chars in Silver
    StructField("payment_installments", StringType(), True),         # cast -> int in Silver
    StructField("payment_value", StringType(), True),                # clean nulls/specials -> double in Silver
])

# One row per customer review; comment fields are free text (may contain newlines/quotes).
REVIEWS_SCHEMA = StructType([
    StructField("review_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("review_score", StringType(), True),                 # cast -> int (default 0) in Silver
    StructField("review_comment_title", StringType(), True),
    StructField("review_comment_message", StringType(), True),
    StructField("review_creation_date", StringType(), True),
    StructField("review_answer_timestamp", StringType(), True),
])

# One row per product, with category and physical dimensions.
# Note: two source columns keep Olist's original "lenght" misspelling (kept as-is to match the file).
PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_category_name", StringType(), True),
    StructField("product_name_lenght", StringType(), True),          # raw Olist typo: "lenght"
    StructField("product_description_lenght", StringType(), True),   # raw Olist typo: "lenght"
    StructField("product_photos_qty", StringType(), True),
    StructField("product_weight_g", StringType(), True),
    StructField("product_length_cm", StringType(), True),
    StructField("product_height_cm", StringType(), True),
    StructField("product_width_cm", StringType(), True),
])

# One row per seller (marketplace merchant) with their location.
SELLERS_SCHEMA = StructType([
    StructField("seller_id", StringType(), True),
    StructField("seller_zip_code_prefix", StringType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True),
])

# Maps zip-code prefixes to lat/lng coordinates and city/state (used to enrich locations).
GEOLOCATION_SCHEMA = StructType([
    StructField("geolocation_zip_code_prefix", StringType(), True),
    StructField("geolocation_lat", StringType(), True),
    StructField("geolocation_lng", StringType(), True),
    StructField("geolocation_city", StringType(), True),
    StructField("geolocation_state", StringType(), True),
])

# Translates Portuguese category names to English (a small lookup/reference table).
CATEGORY_TRANSLATION_SCHEMA = StructType([
    StructField("product_category_name", StringType(), True),
    StructField("product_category_name_english", StringType(), True),
])

# Registry consumed by the parametrised Bronze ingestion notebook.
# Maps a short dataset name -> its schema, so one generic notebook can ingest any source
# by looking its schema up here (instead of hard-coding a schema per notebook).
SOURCE_SCHEMAS = {
    "customers": CUSTOMERS_SCHEMA,
    "orders": ORDERS_SCHEMA,
    "order_items": ORDER_ITEMS_SCHEMA,
    "payments": PAYMENTS_SCHEMA,
    "reviews": REVIEWS_SCHEMA,
    "products": PRODUCTS_SCHEMA,
    "sellers": SELLERS_SCHEMA,
    "geolocation": GEOLOCATION_SCHEMA,
    "category_translation": CATEGORY_TRANSLATION_SCHEMA,
}

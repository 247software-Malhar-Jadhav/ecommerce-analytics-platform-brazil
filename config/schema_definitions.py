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

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType,
)

# ---------------------------------------------------------------------------
# Each schema mirrors the raw CSV header exactly. We keep numeric/money columns
# as StringType in Bronze when the raw file may contain "$" or junk characters,
# so that Bronze never drops a row it cannot cast. Silver does the real casting.
# ---------------------------------------------------------------------------

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_unique_id", StringType(), True),
    StructField("customer_zip_code_prefix", StringType(), True),  # cast -> int in Silver (strip spaces)
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True),
])

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

ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("order_item_id", StringType(), True),                # cast -> int in Silver
    StructField("product_id", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("shipping_limit_date", StringType(), True),          # cast -> timestamp in Silver
    StructField("price", StringType(), True),                        # strip "$" -> double in Silver
    StructField("freight_value", StringType(), True),                # strip "$" -> double in Silver
])

PAYMENTS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("payment_sequential", StringType(), True),           # cast -> int in Silver
    StructField("payment_type", StringType(), True),                 # strip special chars in Silver
    StructField("payment_installments", StringType(), True),         # cast -> int in Silver
    StructField("payment_value", StringType(), True),                # clean nulls/specials -> double in Silver
])

REVIEWS_SCHEMA = StructType([
    StructField("review_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("review_score", StringType(), True),                 # cast -> int (default 0) in Silver
    StructField("review_comment_title", StringType(), True),
    StructField("review_comment_message", StringType(), True),
    StructField("review_creation_date", StringType(), True),
    StructField("review_answer_timestamp", StringType(), True),
])

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

SELLERS_SCHEMA = StructType([
    StructField("seller_id", StringType(), True),
    StructField("seller_zip_code_prefix", StringType(), True),
    StructField("seller_city", StringType(), True),
    StructField("seller_state", StringType(), True),
])

GEOLOCATION_SCHEMA = StructType([
    StructField("geolocation_zip_code_prefix", StringType(), True),
    StructField("geolocation_lat", StringType(), True),
    StructField("geolocation_lng", StringType(), True),
    StructField("geolocation_city", StringType(), True),
    StructField("geolocation_state", StringType(), True),
])

CATEGORY_TRANSLATION_SCHEMA = StructType([
    StructField("product_category_name", StringType(), True),
    StructField("product_category_name_english", StringType(), True),
])

# Registry consumed by the parametrised Bronze ingestion notebook.
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

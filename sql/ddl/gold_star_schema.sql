-- =====================================================================
-- Gold star-schema DDL (reference). The notebooks create these tables
-- programmatically; this file documents the intended physical contract
-- and can be used to pre-create tables or recreate them on a new metastore.
-- Catalog/schema: ecommerce.gold
-- =====================================================================

CREATE CATALOG IF NOT EXISTS ecommerce;
CREATE SCHEMA IF NOT EXISTS ecommerce.gold;

-- ------------------------- DIMENSIONS --------------------------------

-- dim_customer (SCD-1)
CREATE TABLE IF NOT EXISTS ecommerce.gold.dim_customer (
  customer_sk              BIGINT   COMMENT 'Surrogate key = xxhash64(customer_id)',
  customer_id              STRING   COMMENT 'Business key (PK)',
  customer_unique_id       STRING,
  customer_zip_code_prefix INT,
  customer_city            STRING,
  customer_state           STRING,
  updated_ts               TIMESTAMP
) USING DELTA
COMMENT 'SCD-1 customer dimension';

-- dim_product (SCD-1)
CREATE TABLE IF NOT EXISTS ecommerce.gold.dim_product (
  product_sk                     BIGINT,
  product_id                     STRING COMMENT 'Business key (PK)',
  product_category_name          STRING,
  product_category_name_english  STRING,
  product_name_length            INT,
  product_description_length     INT,
  product_photos_qty             INT,
  product_weight_g               INT,
  product_length_cm              INT,
  product_height_cm              INT,
  product_width_cm               INT,
  updated_ts                     TIMESTAMP
) USING DELTA
COMMENT 'SCD-1 product dimension with English category';

-- dim_seller (SCD-2)
CREATE TABLE IF NOT EXISTS ecommerce.gold.dim_seller (
  seller_sk              BIGINT  COMMENT 'Surrogate key, unique per version',
  seller_id              STRING  COMMENT 'Business key',
  seller_zip_code_prefix INT,
  seller_city            STRING,
  seller_state           STRING,
  effective_from         TIMESTAMP,
  effective_to           TIMESTAMP,
  is_current             BOOLEAN
) USING DELTA
COMMENT 'SCD-2 seller dimension tracking location history';

-- dim_category (SCD-0 static)
CREATE TABLE IF NOT EXISTS ecommerce.gold.dim_category (
  category_sk                    BIGINT,
  product_category_name          STRING,
  product_category_name_english  STRING,
  updated_ts                     TIMESTAMP
) USING DELTA
COMMENT 'SCD-0 static category reference';

-- dim_date (derived)
CREATE TABLE IF NOT EXISTS ecommerce.gold.dim_date (
  date_id       INT  COMMENT 'yyyyMMdd surrogate key',
  date          DATE,
  year          INT,
  quarter       INT,
  month         INT,
  month_name    STRING,
  day           INT,
  day_of_week   INT,
  day_name      STRING,
  week_of_year  INT,
  is_weekend    BOOLEAN,
  year_month    STRING
) USING DELTA
COMMENT 'Derived calendar dimension';

-- dim_geolocation (optional, SCD-1)
CREATE TABLE IF NOT EXISTS ecommerce.gold.dim_geolocation (
  geolocation_sk              BIGINT,
  geolocation_zip_code_prefix INT,
  geolocation_lat             DOUBLE,
  geolocation_lng             DOUBLE,
  geolocation_city            STRING,
  geolocation_state           STRING,
  updated_ts                  TIMESTAMP
) USING DELTA
COMMENT 'Optional geolocation dimension';

-- ---------------------------- FACT -----------------------------------

-- fact_orders (grain: order line item)
CREATE TABLE IF NOT EXISTS ecommerce.gold.fact_orders (
  order_id             STRING,
  order_item_id        INT,
  product_id           STRING,
  customer_id          STRING,
  seller_id            STRING,
  seller_sk            BIGINT,
  customer_sk          BIGINT,
  product_sk           BIGINT,
  date_id              INT,
  order_status         STRING,
  price                DOUBLE,
  freight_value        DOUBLE,
  payment_value        DOUBLE,
  payment_installments INT,
  payment_type         STRING,
  review_score         INT,
  delivery_days        INT,
  order_year_month     STRING,
  updated_ts           TIMESTAMP
) USING DELTA
PARTITIONED BY (order_year_month)
COMMENT 'Star-schema fact at order-line-item grain';

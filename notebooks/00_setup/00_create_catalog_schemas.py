# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Environment Setup — Catalog & Schemas
# MAGIC
# MAGIC Run **once per environment** (or let it run idempotently at the head of the Workflow).
# MAGIC Creates the Unity Catalog `ecommerce` and the three medallion schemas
# MAGIC (`bronze`, `silver`, `gold`). Uses managed tables on the metastore default storage;
# MAGIC switch to `MANAGED LOCATION` clauses if you pin each schema to its own ADLS container.

# COMMAND ----------

# MAGIC %run ../04_utils/common_functions

# COMMAND ----------

# Load the project config (JSON) and pull out the names we need.
# Keeping names in config (not hard-coded) lets us point at different environments easily.
cfg = load_config()
catalog = cfg["environment"]["catalog"]          # top-level Unity Catalog container
bronze = cfg["environment"]["bronze_schema"]     # schema for raw landed data
silver = cfg["environment"]["silver_schema"]     # schema for cleaned/conformed data
gold = cfg["environment"]["gold_schema"]         # schema for business-ready dims/facts

# COMMAND ----------

# "IF NOT EXISTS" makes this safe to re-run (idempotent): existing objects are left untouched.
# Create the catalog first, then the three medallion-layer schemas inside it.
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{bronze}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold}")

# Confirm what was created so the run output is easy to verify.
print(f"Catalog '{catalog}' ready with schemas: {bronze}, {silver}, {gold}")
display(spark.sql(f"SHOW SCHEMAS IN {catalog}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notes
# MAGIC * If you are on the legacy Hive metastore instead of Unity Catalog, replace
# MAGIC   `ecommerce.bronze.<table>` references with `bronze.<table>` and create the
# MAGIC   databases with `CREATE DATABASE IF NOT EXISTS bronze` etc.
# MAGIC * Delta time travel, `OPTIMIZE`, `VACUUM` and `MERGE` all work identically in both.

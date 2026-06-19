# Databricks notebook source
# MAGIC %md
# MAGIC # Orchestration · Run Gold for all tables
# MAGIC Dimensions first (fact_orders depends on dim_seller's current surrogate keys),
# MAGIC then the fact. Order matters here, so the list is explicit.

# COMMAND ----------

# Dimensions must complete before the fact (fact joins dim_seller for seller_sk).
gold_dims = [
    "gold_dim_customer", "gold_dim_product", "gold_dim_seller_scd2",
    "gold_dim_category", "gold_dim_date", "gold_dim_geolocation",
]
for nb in gold_dims:
    print(f"==> Gold dim: {nb}")
    dbutils.notebook.run(f"../03_gold/{nb}", 3600)

print("==> Gold fact: gold_fact_orders")
dbutils.notebook.run("../03_gold/gold_fact_orders", 3600)

dbutils.notebook.exit("gold_complete")

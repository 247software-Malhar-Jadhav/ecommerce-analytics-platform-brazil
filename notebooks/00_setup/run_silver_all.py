# Databricks notebook source
# MAGIC %md
# MAGIC # Orchestration · Run Silver for all datasets
# MAGIC Runs every `02_silver/silver_*` notebook. Order is independent (each reads its own
# MAGIC Bronze table and overwrites its own Silver table).

# COMMAND ----------

# The list of Silver notebooks to run. Each one cleans one Bronze table into a Silver table.
# Order does not matter here because the notebooks are independent of one another.
silver_notebooks = [
    "silver_customers", "silver_orders", "silver_order_items", "silver_payments",
    "silver_reviews", "silver_products", "silver_sellers", "silver_geolocation",
    "silver_category_translation",
]

# Run each Silver notebook in turn (3600 = per-notebook timeout in seconds).
for nb in silver_notebooks:
    print(f"==> Silver: {nb}")
    dbutils.notebook.run(f"../02_silver/{nb}", 3600)

# Signal success to whatever orchestrator called this notebook.
dbutils.notebook.exit("silver_complete")

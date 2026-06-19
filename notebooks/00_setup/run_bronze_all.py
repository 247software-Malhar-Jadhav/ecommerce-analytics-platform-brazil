# Databricks notebook source
# MAGIC %md
# MAGIC # Orchestration · Run Bronze for all datasets
# MAGIC Loops over every source in `pipeline_config.json` and runs `01_bronze/bronze_ingestion`
# MAGIC once per dataset. Called by ADF (single Databricks Notebook activity) or by a
# MAGIC Databricks Workflow task. Sequential by default; safe because each writes its own table.

# COMMAND ----------

# MAGIC %run ../04_utils/common_functions

# COMMAND ----------

import json
cfg = load_config()
results = {}
for s in cfg["sources"]:
    name = s["name"]
    print(f"==> Bronze ingest: {name}")
    out = dbutils.notebook.run("../01_bronze/bronze_ingestion", 3600, {"dataset": name})
    results[name] = out
    print(f"    {out}")

print(json.dumps(results, indent=2))
dbutils.notebook.exit(json.dumps(results))

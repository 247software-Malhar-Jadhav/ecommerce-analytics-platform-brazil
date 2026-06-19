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
# Load config and prepare a dict to collect each dataset's result for reporting.
cfg = load_config()
results = {}
# Loop over every source defined in config and run the SAME bronze notebook for each.
for s in cfg["sources"]:
    name = s["name"]
    print(f"==> Bronze ingest: {name}")
    # Run the generic ingestion notebook, passing the dataset name as a parameter (widget).
    # 3600 = timeout in seconds; the notebook returns a string via dbutils.notebook.exit(...).
    out = dbutils.notebook.run("../01_bronze/bronze_ingestion", 3600, {"dataset": name})
    results[name] = out          # remember this dataset's returned status
    print(f"    {out}")

# Print a combined summary, then return it so a parent job/orchestrator can read it.
print(json.dumps(results, indent=2))
dbutils.notebook.exit(json.dumps(results))

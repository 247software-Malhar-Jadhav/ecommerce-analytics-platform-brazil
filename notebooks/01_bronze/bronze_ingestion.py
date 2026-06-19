# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze Layer — Raw Ingestion
# MAGIC
# MAGIC **Goal:** store raw source data *exactly as received*. The only change Bronze makes is
# MAGIC adding an `ingest_ts` audit column and a `source_file` lineage column.
# MAGIC
# MAGIC Rules honoured (from spec):
# MAGIC 1. Read source CSVs with an explicit schema (no inference, no data change)
# MAGIC 2. Add ingestion timestamp column
# MAGIC 3. Write in **Delta** format
# MAGIC 4. Use **append** mode (full history of every daily file is retained)
# MAGIC 5. Do **NOT** deduplicate / filter / join / rename
# MAGIC
# MAGIC The notebook is **parametrised** — ADF / Databricks Workflows pass the `dataset` widget,
# MAGIC so one notebook ingests all nine sources. The landing folder is written by ADF's
# MAGIC Copy activity before this task runs.

# COMMAND ----------

# MAGIC %run ../04_utils/common_functions

# COMMAND ----------

# MAGIC %run ../../config/schema_definitions

# COMMAND ----------

# Fully dynamic: pass EITHER the short dataset name (e.g. "customers")
# OR the raw file name (e.g. "olist_customers_dataset.csv"). The notebook
# resolves the other values from config/pipeline_config.json automatically.
dbutils.widgets.text("dataset", "customers", "Dataset name OR raw file name")
dbutils.widgets.text("file_name", "", "Raw file name (optional; overrides 'dataset')")
dbutils.widgets.text("run_date", "", "Logical run date (yyyy-MM-dd, optional)")

dataset_input = dbutils.widgets.get("dataset").strip()
file_input = dbutils.widgets.get("file_name").strip()
run_date = dbutils.widgets.get("run_date")

cfg = load_config()
catalog = cfg["environment"]["catalog"]
bronze_schema = cfg["environment"]["bronze_schema"]
landing_path = cfg["storage"]["landing_path"]


def resolve_source(cfg, dataset_input, file_input):
    """Resolve a config source from EITHER a dataset name OR a raw file name.

    Priority: explicit file_name widget > a dataset value that is itself a file name
    > a dataset value that is a logical name. This is what makes the notebook
    'put a file name and it lands in Bronze' dynamic, while still resolving the
    explicit schema + target table the file needs.
    """
    key = file_input or dataset_input
    for s in cfg["sources"]:
        # match on logical name OR on the configured file name (with/without .csv)
        if key == s["name"] or key == s["file"] or key == s["file"].replace(".csv", ""):
            return s
    raise ValueError(
        f"Could not resolve '{key}'. Pass a dataset name {[s['name'] for s in cfg['sources']]} "
        f"or a file name listed in config/pipeline_config.json."
    )


source = resolve_source(cfg, dataset_input, file_input)
dataset = source["name"]                 # canonical logical name (also the bronze table name)
schema = SOURCE_SCHEMAS[dataset]
source_file = source["file"]
src_path = f"{landing_path}/{source_file}"
target_table = f"{catalog}.{bronze_schema}.{dataset}"

print(f"Ingesting '{dataset}' (file: {source_file})\n  from : {src_path}\n  into : {target_table}")

# COMMAND ----------

# MAGIC %md ### Read raw CSV (explicit schema, nothing changed)

# COMMAND ----------

raw = read_csv(spark, src_path, schema)

# Add audit/lineage columns ONLY. No casting, no rename, no filter.
bronze_df = add_ingest_ts(raw).withColumn("source_file", F.lit(source_file))

print(f"Rows read: {bronze_df.count():,}")
display(bronze_df.limit(5))

# COMMAND ----------

# MAGIC %md ### Append to Bronze Delta
# MAGIC Append mode keeps every daily snapshot, so Bronze itself becomes the immutable
# MAGIC audit log. Downstream Silver deduplicates to the latest version per key.

# COMMAND ----------

write_delta(bronze_df, target_table, mode="append", merge_schema=True)

# COMMAND ----------

# MAGIC %md ### Register table comment + show history

# COMMAND ----------

spark.sql(f"COMMENT ON TABLE {target_table} IS 'Bronze raw ingest of {source_file} — append-only, ingest_ts added, no transforms.'")
display(spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 5"))

# COMMAND ----------

# Return a small JSON result so the orchestrator (ADF/Workflows) can log row counts.
dbutils.notebook.exit(f'{{"dataset":"{dataset}","rows":{bronze_df.count()},"table":"{target_table}"}}')

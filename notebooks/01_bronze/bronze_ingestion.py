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

# Pull in shared helper functions (read_csv, add_ingest_ts, write_delta, load_config, F, ...).
# %run executes that notebook here so its functions become available in this one.
# MAGIC %run ../04_utils/common_functions

# COMMAND ----------

# Pull in the explicit column schemas for every source (SOURCE_SCHEMAS dict).
# Bronze reads with a fixed schema instead of inferring it, so the raw data is never altered.
# MAGIC %run ../../config/schema_definitions

# COMMAND ----------

# Fully dynamic: pass EITHER the short dataset name (e.g. "customers")
# OR the raw file name (e.g. "olist_customers_dataset.csv"). The notebook
# resolves the other values from config/pipeline_config.json automatically.
# Widgets are Databricks input parameters: an orchestrator (ADF / Workflows) sets these
# at runtime, so one notebook can ingest all nine sources instead of nine copies.
dbutils.widgets.text("dataset", "customers", "Dataset name OR raw file name")
dbutils.widgets.text("file_name", "", "Raw file name (optional; overrides 'dataset')")
dbutils.widgets.text("run_date", "", "Logical run date (yyyy-MM-dd, optional)")

# Read the values the orchestrator passed in; .strip() removes accidental whitespace.
dataset_input = dbutils.widgets.get("dataset").strip()
file_input = dbutils.widgets.get("file_name").strip()
run_date = dbutils.widgets.get("run_date")

# Load the central pipeline config (catalog/schema names, paths, source list) once.
# Keeping these in config means no hard-coded paths scattered through the code.
cfg = load_config()
catalog = cfg["environment"]["catalog"]          # Unity Catalog name (top-level namespace)
bronze_schema = cfg["environment"]["bronze_schema"]  # schema/database that holds Bronze tables
landing_path = cfg["storage"]["landing_path"]    # cloud folder where ADF dropped the raw CSVs


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


# Turn whatever the user passed into the one config entry it refers to,
# then derive every concrete value (logical name, schema, file path, target table) from it.
source = resolve_source(cfg, dataset_input, file_input)
dataset = source["name"]                 # canonical logical name (also the bronze table name)
schema = SOURCE_SCHEMAS[dataset]         # the explicit column types for this source
source_file = source["file"]             # the raw CSV file name in the landing folder
src_path = f"{landing_path}/{source_file}"   # full path to read the CSV from
# Three-part name catalog.schema.table is how tables are addressed in Unity Catalog.
target_table = f"{catalog}.{bronze_schema}.{dataset}"

print(f"Ingesting '{dataset}' (file: {source_file})\n  from : {src_path}\n  into : {target_table}")

# COMMAND ----------

# MAGIC %md ### Read raw CSV (explicit schema, nothing changed)

# COMMAND ----------

# Read the CSV using the fixed schema (no schema inference) so the data is loaded exactly as-is.
raw = read_csv(spark, src_path, schema)

# Add audit/lineage columns ONLY. No casting, no rename, no filter.
# ingest_ts = when we loaded the row (audit); source_file = which file it came from (lineage).
# F.lit(...) writes the same constant file name into every row.
bronze_df = add_ingest_ts(raw).withColumn("source_file", F.lit(source_file))

# .count() triggers Spark to actually run; useful as a quick sanity check on volume.
print(f"Rows read: {bronze_df.count():,}")
display(bronze_df.limit(5))

# COMMAND ----------

# MAGIC %md ### Append to Bronze Delta
# MAGIC Append mode keeps every daily snapshot, so Bronze itself becomes the immutable
# MAGIC audit log. Downstream Silver deduplicates to the latest version per key.

# COMMAND ----------

# Write as Delta in append mode so each run adds to (never overwrites) prior snapshots.
# merge_schema lets new/changed columns be absorbed without the write failing.
write_delta(bronze_df, target_table, mode="append", merge_schema=True)

# COMMAND ----------

# MAGIC %md ### Register table comment + show history

# COMMAND ----------

# Attach a human-readable description to the table so anyone browsing the catalog knows its purpose.
spark.sql(f"COMMENT ON TABLE {target_table} IS 'Bronze raw ingest of {source_file} — append-only, ingest_ts added, no transforms.'")
# DESCRIBE HISTORY shows Delta's version log (each append is a new version) — handy for auditing.
display(spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 5"))

# COMMAND ----------

# Return a small JSON result so the orchestrator (ADF/Workflows) can log row counts.
# dbutils.notebook.exit ends the notebook and hands this string back to whoever called it.
dbutils.notebook.exit(f'{{"dataset":"{dataset}","rows":{bronze_df.count()},"table":"{target_table}"}}')

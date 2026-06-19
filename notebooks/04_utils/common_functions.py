# Databricks notebook source
# MAGIC %md
# MAGIC # Common Utility Functions
# MAGIC
# MAGIC Reusable, side-effect-light helpers shared by Bronze / Silver / Gold notebooks.
# MAGIC Implements the "Function & Utility Notebooks" requirement from the spec:
# MAGIC   1. Read data function
# MAGIC   2. Write Delta function
# MAGIC   3. Deduplication function
# MAGIC   4. Null handling function
# MAGIC   5. Schema validation function
# MAGIC   + extras (config loader, audit columns, secure mount helper, table existence check).
# MAGIC
# MAGIC Import into other notebooks with:  `%run ../04_utils/common_functions`

# COMMAND ----------

import json
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, TimestampType
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md ### Config loader

# COMMAND ----------

def load_config(path: str = "../../config/pipeline_config.json") -> dict:
    """Load the pipeline config JSON.

    On Databricks Repos the config travels with the repo, so a relative path resolves.
    When running as a Workflow task we fall back to a workspace path passed via widget.
    """
    try:
        with open(path, "r") as fh:
            return json.load(fh)
    except FileNotFoundError:
        # Fallback for Workflow context where CWD differs.
        with open("/Workspace/Repos/ecommerce/config/pipeline_config.json", "r") as fh:
            return json.load(fh)

# COMMAND ----------

# MAGIC %md ### 1. Read data function

# COMMAND ----------

def read_csv(spark: SparkSession, path: str, schema: StructType, header: bool = True,
             multiline: bool = True) -> DataFrame:
    """Read a raw CSV with an explicit schema (no inferSchema).

    `_rescued_data` captures any field that does not fit the schema, so Bronze never
    silently drops or corrupts a malformed row. `multiline`/`escape` handle the free-text
    review-comment columns which legitimately contain embedded newlines and quotes.
    """
    return (
        spark.read
        .format("csv")
        .option("header", header)
        .option("multiLine", multiline)
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_rescued_data")
        .schema(schema)
        .load(path)
    )


def read_delta(spark: SparkSession, table_or_path: str) -> DataFrame:
    """Read a Delta table either by Unity Catalog name (catalog.schema.table) or by path."""
    if table_or_path.startswith(("abfss://", "dbfs:/", "/")):
        return spark.read.format("delta").load(table_or_path)
    return spark.read.table(table_or_path)


def read_delta_version(spark: SparkSession, table_or_path: str, version: int = None,
                       timestamp: str = None) -> DataFrame:
    """Delta TIME TRAVEL read. Provide exactly one of `version` or `timestamp`.

    Example: read_delta_version(spark, "ecommerce.silver.sellers", version=3)
             read_delta_version(spark, "ecommerce.gold.fact_orders", timestamp="2026-06-18")
    """
    reader = spark.read.format("delta")
    if version is not None:
        reader = reader.option("versionAsOf", version)
    elif timestamp is not None:
        reader = reader.option("timestampAsOf", timestamp)
    else:
        raise ValueError("read_delta_version requires either `version` or `timestamp`.")
    if table_or_path.startswith(("abfss://", "dbfs:/", "/")):
        return reader.load(table_or_path)
    return reader.table(table_or_path)

# COMMAND ----------

# MAGIC %md ### 2. Write Delta function

# COMMAND ----------

def write_delta(df: DataFrame, target: str, mode: str = "append",
                partition_by=None, merge_schema: bool = True,
                optimize_write: bool = True) -> None:
    """Write a DataFrame to Delta by Unity Catalog table name or path.

    mode: 'append' (Bronze, fact) | 'overwrite' (Silver, SCD-1 dims).
    `mergeSchema` lets new source columns flow through without a manual ALTER TABLE.
    """
    writer = df.write.format("delta").mode(mode)
    if optimize_write:
        writer = writer.option("delta.autoOptimize.optimizeWrite", "true")
    if merge_schema and mode in ("append", "overwrite"):
        writer = writer.option("mergeSchema", "true")
    if partition_by:
        writer = writer.partitionBy(partition_by if isinstance(partition_by, list) else [partition_by])
    if target.startswith(("abfss://", "dbfs:/", "/")):
        writer.save(target)
    else:
        writer.saveAsTable(target)

# COMMAND ----------

# MAGIC %md ### 3. Deduplication function

# COMMAND ----------

def deduplicate(df: DataFrame, keys, order_col: str = None, descending: bool = True) -> DataFrame:
    """Keep one row per business key.

    If `order_col` is given, keep the latest row by that column (e.g. ingest_ts) — useful
    for incremental loads where the same key may arrive in multiple daily files.
    """
    keys = keys if isinstance(keys, list) else [keys]
    if order_col is None:
        return df.dropDuplicates(keys)
    order = F.col(order_col).desc() if descending else F.col(order_col).asc()
    w = Window.partitionBy(*keys).orderBy(order)
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )

# COMMAND ----------

# MAGIC %md ### 4. Null handling function

# COMMAND ----------

def drop_null_keys(df: DataFrame, keys) -> DataFrame:
    """Drop rows where any primary/business key column is null or blank."""
    keys = keys if isinstance(keys, list) else [keys]
    cond = None
    for k in keys:
        c = F.col(k).isNotNull() & (F.trim(F.col(k)) != "")
        cond = c if cond is None else (cond & c)
    return df.filter(cond)


def fill_defaults(df: DataFrame, defaults: dict) -> DataFrame:
    """Fill nulls with explicit defaults, e.g. {'review_score': 0}."""
    return df.fillna(defaults)


def trim_strings(df: DataFrame) -> DataFrame:
    """Trim leading/trailing whitespace from every string column."""
    for f in df.schema.fields:
        if f.dataType.simpleString() == "string":
            df = df.withColumn(f.name, F.trim(F.col(f.name)))
    return df

# COMMAND ----------

# MAGIC %md ### 5. Schema validation function

# COMMAND ----------

def validate_schema(df: DataFrame, expected: StructType, strict: bool = False) -> bool:
    """Validate a DataFrame against an expected schema.

    strict=False  -> every expected (name, type) must be present (extra cols allowed).
    strict=True   -> names + types must match exactly.
    Raises ValueError on mismatch so the job fails loudly instead of writing bad data.
    """
    actual = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    expect = {f.name: f.dataType.simpleString() for f in expected.fields}
    missing = [c for c in expect if c not in actual]
    if missing:
        raise ValueError(f"Schema validation failed. Missing columns: {missing}")
    type_mismatch = [c for c in expect if actual.get(c) != expect[c]]
    if type_mismatch:
        details = {c: {"expected": expect[c], "actual": actual.get(c)} for c in type_mismatch}
        raise ValueError(f"Schema validation failed. Type mismatch: {details}")
    if strict:
        extra = [c for c in actual if c not in expect]
        if extra:
            raise ValueError(f"Strict schema validation failed. Unexpected columns: {extra}")
    return True

# COMMAND ----------

# MAGIC %md ### Extras

# COMMAND ----------

def add_audit_columns(df: DataFrame, source_file: str = None) -> DataFrame:
    """Add lineage/audit columns used across layers."""
    out = df.withColumn("updated_ts", F.current_timestamp())
    if source_file is not None:
        out = out.withColumn("source_file", F.lit(source_file))
    return out


def add_ingest_ts(df: DataFrame) -> DataFrame:
    """Add the Bronze ingestion timestamp required by the spec for every dataset."""
    return df.withColumn("ingest_ts", F.current_timestamp())


def table_exists(spark: SparkSession, table_name: str) -> bool:
    """True if a Unity Catalog managed table exists."""
    try:
        return spark.catalog.tableExists(table_name)
    except Exception:
        return False


def clean_money(col):
    """Column expression: strip '$', thousands separators and spaces, then cast to double."""
    cleaned = F.regexp_replace(F.col(col), r"[^0-9.\-]", "")
    return F.when(cleaned == "", None).otherwise(cleaned.cast("double"))


def clean_special_chars(col):
    """Column expression: keep only alphanumerics and underscore (e.g. credit@card -> credit_card)."""
    return F.regexp_replace(F.lower(F.trim(F.col(col))), r"[^a-z0-9]+", "_")

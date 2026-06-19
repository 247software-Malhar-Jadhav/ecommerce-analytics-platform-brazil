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

# Standard library: used to read the JSON config file from disk.
import json
# Core Spark types. DataFrame = a distributed table; SparkSession = entry point to Spark.
from pyspark.sql import DataFrame, SparkSession
# `functions as F` is the conventional alias for Spark's built-in column functions (F.col, F.lit, ...).
from pyspark.sql import functions as F
# StructType describes a table's schema (its columns + data types).
from pyspark.sql.types import StructType, TimestampType
# Window lets us run "per-group" calculations (e.g. rank rows within each key) — used in dedup.
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
        # Try the relative path first (works when running inside a Databricks Repo).
        with open(path, "r") as fh:
            return json.load(fh)  # parse the JSON text into a Python dict
    except FileNotFoundError:
        # Fallback for Workflow context where CWD differs.
        # If the relative path is not found, try a fixed absolute workspace path instead.
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
        .format("csv")                                  # tell Spark the source is CSV
        .option("header", header)                       # first line holds column names
        .option("multiLine", multiline)                 # allow a single field to span multiple lines
        .option("quote", '"')                           # double-quote marks the start/end of a field
        .option("escape", '"')                          # a doubled "" inside a quoted field is a literal quote
        .option("mode", "PERMISSIVE")                   # do not fail on bad rows; keep them and flag them
        .option("columnNameOfCorruptRecord", "_rescued_data")  # bad/unparseable data lands in this column
        .schema(schema)                                 # use OUR schema instead of inferring (faster + deterministic)
        .load(path)                                     # actually read the file(s) at this path
    )


def read_delta(spark: SparkSession, table_or_path: str) -> DataFrame:
    """Read a Delta table either by Unity Catalog name (catalog.schema.table) or by path."""
    # If the argument looks like a storage path, load it as files; otherwise treat it as a table name.
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
    # Delta keeps a full history of every write, so we can ask for an older snapshot.
    if version is not None:
        reader = reader.option("versionAsOf", version)        # read the table as of a specific version number
    elif timestamp is not None:
        reader = reader.option("timestampAsOf", timestamp)    # read the table as it looked at a point in time
    else:
        # Neither was provided — fail loudly so the caller fixes the mistake.
        raise ValueError("read_delta_version requires either `version` or `timestamp`.")
    # Same path-vs-name decision as read_delta above.
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
    # Start building the write: Delta format, with the chosen save mode (append/overwrite).
    writer = df.write.format("delta").mode(mode)
    if optimize_write:
        # Ask Delta to combine many small files into fewer bigger ones as it writes (faster reads later).
        writer = writer.option("delta.autoOptimize.optimizeWrite", "true")
    if merge_schema and mode in ("append", "overwrite"):
        # Allow new columns in the DataFrame to be added to the table automatically.
        writer = writer.option("mergeSchema", "true")
    if partition_by:
        # Physically split the table's files by these column(s) so queries can skip irrelevant data.
        writer = writer.partitionBy(partition_by if isinstance(partition_by, list) else [partition_by])
    # If target is a storage path, write files there; otherwise register it as a catalog table.
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
    # Accept a single key string or a list; normalise to a list so the rest of the code is uniform.
    keys = keys if isinstance(keys, list) else [keys]
    if order_col is None:
        # No tie-breaker given: just drop exact duplicate rows on the key columns.
        return df.dropDuplicates(keys)
    # With a tie-breaker: define sort direction (newest first by default).
    order = F.col(order_col).desc() if descending else F.col(order_col).asc()
    # Group rows by the business key, ordered so the "best" row comes first within each group.
    w = Window.partitionBy(*keys).orderBy(order)
    return (
        df.withColumn("_rn", F.row_number().over(w))  # number rows 1,2,3... inside each key group
          .filter(F.col("_rn") == 1)                  # keep only the first (best) row per key
          .drop("_rn")                                # remove the helper column
    )

# COMMAND ----------

# MAGIC %md ### 4. Null handling function

# COMMAND ----------

def drop_null_keys(df: DataFrame, keys) -> DataFrame:
    """Drop rows where any primary/business key column is null or blank."""
    # Normalise to a list so we can loop over one or many key columns.
    keys = keys if isinstance(keys, list) else [keys]
    cond = None
    for k in keys:
        # For each key: the value must be non-null AND not just whitespace.
        c = F.col(k).isNotNull() & (F.trim(F.col(k)) != "")
        # Combine conditions with AND so a row is kept only if EVERY key is valid.
        cond = c if cond is None else (cond & c)
    return df.filter(cond)


def fill_defaults(df: DataFrame, defaults: dict) -> DataFrame:
    """Fill nulls with explicit defaults, e.g. {'review_score': 0}."""
    return df.fillna(defaults)


def trim_strings(df: DataFrame) -> DataFrame:
    """Trim leading/trailing whitespace from every string column."""
    # Walk through each column in the schema...
    for f in df.schema.fields:
        # ...and only trim the ones that are text (string) columns.
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
    # Build simple {column_name: type} maps for the real DataFrame and the expected schema.
    actual = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    expect = {f.name: f.dataType.simpleString() for f in expected.fields}
    # Any expected column that is not present at all is an error.
    missing = [c for c in expect if c not in actual]
    if missing:
        raise ValueError(f"Schema validation failed. Missing columns: {missing}")
    # Columns that exist but have the wrong data type are also an error.
    type_mismatch = [c for c in expect if actual.get(c) != expect[c]]
    if type_mismatch:
        # Collect a clear before/after report to make debugging easy.
        details = {c: {"expected": expect[c], "actual": actual.get(c)} for c in type_mismatch}
        raise ValueError(f"Schema validation failed. Type mismatch: {details}")
    if strict:
        # In strict mode, even unexpected EXTRA columns are not allowed.
        extra = [c for c in actual if c not in expect]
        if extra:
            raise ValueError(f"Strict schema validation failed. Unexpected columns: {extra}")
    return True  # all checks passed

# COMMAND ----------

# MAGIC %md ### Extras

# COMMAND ----------

def add_audit_columns(df: DataFrame, source_file: str = None) -> DataFrame:
    """Add lineage/audit columns used across layers."""
    # Stamp every row with the time this write happened (helps trace when data changed).
    out = df.withColumn("updated_ts", F.current_timestamp())
    if source_file is not None:
        # Record which source file the data came from (F.lit = a constant value for all rows).
        out = out.withColumn("source_file", F.lit(source_file))
    return out


def add_ingest_ts(df: DataFrame) -> DataFrame:
    """Add the Bronze ingestion timestamp required by the spec for every dataset."""
    # Record exactly when each row was ingested into Bronze (raw landing layer).
    return df.withColumn("ingest_ts", F.current_timestamp())


def table_exists(spark: SparkSession, table_name: str) -> bool:
    """True if a Unity Catalog managed table exists."""
    # Used to decide whether to create vs. merge into a table. Swallow errors and say "no".
    try:
        return spark.catalog.tableExists(table_name)
    except Exception:
        return False


def clean_money(col):
    """Column expression: strip '$', thousands separators and spaces, then cast to double."""
    # Remove every character that is not a digit, a dot, or a minus sign.
    cleaned = F.regexp_replace(F.col(col), r"[^0-9.\-]", "")
    # If nothing is left (empty string), treat it as null; otherwise convert text to a number.
    return F.when(cleaned == "", None).otherwise(cleaned.cast("double"))


def clean_special_chars(col):
    """Column expression: keep only alphanumerics and underscore (e.g. credit@card -> credit_card)."""
    # Lowercase + trim, then replace any run of non-alphanumeric chars with a single underscore.
    return F.regexp_replace(F.lower(F.trim(F.col(col))), r"[^a-z0-9]+", "_")

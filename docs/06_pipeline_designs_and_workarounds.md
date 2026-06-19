# 06 · Pipeline Designs & Workarounds

The **same business outcome** (raw CSV → clean Gold star schema, daily) can be built many ways.
This doc presents **6 end-to-end pipeline designs** (the one we chose + 5 alternatives) and then
**sub-problem workarounds** (ingestion, dedup, SCD-2, scheduling, etc.). Use it to answer
*"how else could you have built this?"* and *"what are the trade-offs?"*.

---

## Pipeline 1 — ADF + Databricks notebooks (THE ONE WE BUILT) ✅

```
 Source CSV ─► ADF Copy ─► ADLS landing ─► ADF triggers Databricks notebooks
                                              │
                                  Bronze ─► Silver ─► Gold ─► DQ   (Delta)
```
**How:** ADF Copy activity lands files, then ADF `DatabricksNotebook` activities run
`run_bronze_all → run_silver_all → run_gold_all → data_quality_checks`. Trigger at 07:00.

| Pros | Cons |
|------|------|
| Clear split: ADF = ingestion/orchestration, Databricks = compute | Two services to operate & monitor |
| ADF has 90+ connectors for real source systems | Notebook-based (imperative) code to maintain |
| Matches the spec exactly ("ingestion with ADF") | Job-cluster spin-up latency per run |
| Easy to add pre-copy validation, file sensing | |

**When to use:** enterprise Azure shops that already standardize ingestion on ADF.

---

## Pipeline 2 — Databricks Workflows only (no ADF)

```
 Source CSV ─► (Auto Loader / dbutils.fs cp) ─► Databricks Workflow job
                          Bronze ─► Silver ─► Gold ─► DQ tasks (DAG)
```
**How:** A Databricks **Job** (`databricks/workflows/job_ecommerce_medallion.json`) with task
dependencies `setup → bronze → silver → gold → data_quality`, cron `0 0 7 * * ?`. Ingestion done
inside Databricks (Auto Loader or a copy task).

| Pros | Cons |
|------|------|
| One platform, one bill, one place to monitor | Fewer source connectors than ADF |
| Native task DAG, retries, alerts, repair-run | Ingestion from on-prem/SaaS sources harder |
| Cheaper (no ADF) | Couples ingestion to compute platform |

**When to use:** Databricks-centric teams; sources already in cloud storage.
*(This project ships this as the alternative orchestrator.)*

---

## Pipeline 3 — Delta Live Tables (DLT) — declarative

```
 Source ─► DLT pipeline (declarative @dlt.table + EXPECTATIONS)
            bronze() ─► silver() ─► gold()   (auto-managed DAG + DQ)
```
**How:** Replace imperative notebooks with `@dlt.table` definitions; DLT infers the DAG, manages
checkpoints, and enforces data quality via `@dlt.expect_or_drop`. Streaming or triggered.

| Pros | Cons |
|------|------|
| Less code; DAG + lineage auto-managed | Less control over exotic logic (manual SCD-2 trickier) |
| Built-in **expectations** (DQ as declarations) | DLT-specific; some vendor lock-in |
| Auto-scaling, auto file management, CDC via `APPLY CHANGES` | Costs/limits of DLT edition |

**Note:** DLT's `APPLY CHANGES INTO ... STORED AS SCD TYPE 2` implements SCD-2 *for you* — a big
simplification over the manual MERGE in `gold_dim_seller_scd2.py`.

**When to use:** want declarative ETL + managed DQ, willing to adopt DLT.

---

## Pipeline 4 — Auto Loader (cloudFiles) incremental ingestion

```
 Files arrive continuously ─► Auto Loader (cloudFiles) ─► Bronze stream
                              (tracks new files via checkpoint) ─► Silver ─► Gold
```
**How:** Instead of ADF full-copy, `spark.readStream.format("cloudFiles")` auto-detects *new*
files, processes only them, and checkpoints progress. Run as triggered batch (`trigger(availableNow=True)`)
daily, or continuously.

| Pros | Cons |
|------|------|
| True incremental — only NEW files processed | Streaming concepts/checkpoints to manage |
| Scales to millions of files; no manual bookkeeping | Overkill if you get one tidy file/day |
| Handles late-arriving files gracefully | Requires schema evolution handling |

**When to use:** high file volume / frequent drops, want exactly-once new-file processing.
*(Listed as a future improvement in `docs/04_pdf_improvements.md`.)*

---

## Pipeline 5 — ADF Mapping Data Flows (low-code, no Databricks)

```
 Source CSV ─► ADF Copy ─► ADLS ─► ADF Mapping Data Flow (visual transforms)
                                   Bronze ─► Silver ─► Gold (Spark under the hood)
```
**How:** ADF **Mapping Data Flows** run Spark transformations through a drag-and-drop UI
(derive columns, aggregate, alter row for upsert, etc.) on ADF-managed Spark — no notebooks.

| Pros | Cons |
|------|------|
| No code; analysts can build/maintain | Harder for complex SCD-2 / custom logic |
| One tool (ADF) end-to-end | Less testable/versionable than code |
| Good for simple standardization | Debugging large flows is painful |

**When to use:** low-code mandate, simpler transforms, no Spark/PySpark skills on team.

---

## Pipeline 6 — Synapse / Warehouse-first ELT (ELT instead of ETL)

```
 Source CSV ─► ADF/COPY INTO ─► staging tables ─► SQL transforms (dbt-style)
                                 Bronze ─► Silver ─► Gold  (all in SQL warehouse)
```
**How:** Load raw into a SQL engine (Synapse dedicated pool / Databricks SQL / Snowflake) and do
all transforms in **SQL** (often with **dbt** for modular models + tests). "EL" then "T".

| Pros | Cons |
|------|------|
| SQL-only team can own it; dbt gives tests+docs+lineage | Heavy semi-structured/streaming less natural |
| Warehouse compute is great for big joins/aggregations | Raw-file landing still needs ADF/COPY |
| Mature ecosystem (dbt) | Another engine to license/run |

**When to use:** SQL-first orgs, dbt adopters, BI-centric warehouses.

---

## Side-by-side comparison

| # | Design | Ingestion | Transform | Orchestrate | Best when |
|---|--------|-----------|-----------|-------------|-----------|
| 1 | **ADF + Databricks** ✅ | ADF Copy | PySpark notebooks | ADF | Enterprise Azure, our spec |
| 2 | Databricks Workflows | Auto Loader/copy | PySpark notebooks | Databricks Jobs | Databricks-centric |
| 3 | Delta Live Tables | DLT/Auto Loader | Declarative `@dlt.table` | DLT | Declarative + managed DQ |
| 4 | Auto Loader | cloudFiles stream | PySpark | Jobs/DLT | High file volume |
| 5 | ADF Mapping Data Flows | ADF Copy | Visual Spark | ADF | Low-code teams |
| 6 | Warehouse ELT (dbt) | COPY INTO | SQL/dbt | dbt/Airflow | SQL-first orgs |

---

## Sub-problem workarounds (same step, multiple techniques)

### A. Getting files into the lake
| Option | Notes |
|--------|-------|
| ADF Copy activity (chosen) | Most connectors; good for many sources |
| Auto Loader `cloudFiles` | Incremental new-file detection + checkpoint |
| `COPY INTO` (SQL) | Simple bulk load into a Delta/warehouse table |
| `dbutils.fs.cp` / azcopy | Quick/manual; fine for demos |
| Event Grid + Functions | Event-driven on file arrival |

### B. Deduplication (Silver)
| Option | Notes |
|--------|-------|
| `row_number()` window, keep rn=1 by `ingest_ts` (chosen) | keeps *latest* per key |
| `dropDuplicates(keys)` | keeps an arbitrary row (no "latest" guarantee) |
| `MERGE` with newer-wins condition | upsert directly into Silver |
| `GROUP BY key, max(ts)` then join | classic SQL approach |

### C. SCD-2 implementation
| Option | Notes |
|--------|-------|
| Manual 2-step MERGE (chosen) | full control; what `gold_dim_seller_scd2.py` does |
| DLT `APPLY CHANGES ... SCD TYPE 2` | declarative, least code |
| Delta **Change Data Feed (CDF)** | drive SCD-2 from upstream row changes |
| Snapshot/append + view picking current | simplest; more storage, query-time logic |
| Hash-diff comparison | detect change by hashing tracked columns |

### D. Incremental fact load
| Option | Notes |
|--------|-------|
| `left_anti` on natural key → append (chosen) | idempotent, simple |
| `MERGE` on natural key | upsert; handles updates too |
| Watermark on `ingest_ts`/date | process only rows since last run |
| Partition overwrite (`replaceWhere`) | reload just affected partitions |

### E. Surrogate keys
| Option | Notes |
|--------|-------|
| `xxhash64(business_key)` (chosen) | deterministic, stable, no coordination |
| Delta `GENERATED ALWAYS AS IDENTITY` | true sequential ints; needs single-writer care |
| `monotonically_increasing_id()` | not stable across runs — avoid for keys |
| `md5`/`sha2` hash | wider key; same idea as xxhash64 |

### F. Scheduling / orchestration
| Option | Notes |
|--------|-------|
| ADF schedule trigger (chosen) | 07:00 daily; tumbling-window also possible |
| Databricks Jobs cron | one-platform alternative |
| Apache Airflow / Azure equiv. | cross-system DAGs, rich dependencies |
| Event/tumbling triggers | run on file arrival or fixed windows |

### G. Data quality
| Option | Notes |
|--------|-------|
| Custom asserts + `dq_results` table (chosen) | simple, explicit, fails job on FATAL |
| DLT **expectations** | declarative drop/quarantine/fail |
| Great Expectations / Soda | rich suites, docs, profiling |
| Delta **CHECK constraints** | enforce at write time (`ALTER TABLE ADD CONSTRAINT`) |

### H. Handling the order_items `$`/dirty money & bad rows
| Option | Notes |
|--------|-------|
| `regexp_replace` strip non-numeric → cast (chosen) | `clean_money` helper |
| `try_cast` | null on failure instead of error |
| Quarantine invalid to `*_errors` (chosen) | dead-letter pattern, nothing silently lost |
| Reject whole file on threshold | strict gate for critical feeds |

---

## How to talk about this in an interview

> *"I chose **ADF + Databricks** because the spec called for ADF ingestion and it cleanly
> separates orchestration from compute. But the same outcome can be reached with a
> **Databricks-only Workflow**, with **Delta Live Tables** for a declarative DAG + built-in
> expectations, with **Auto Loader** for high-volume incremental files, or with a
> **warehouse-first ELT (dbt)** approach for SQL teams. For SCD-2 I hand-wrote a 2-step MERGE
> for full control, but DLT's `APPLY CHANGES … SCD TYPE 2` would remove most of that code."*
```

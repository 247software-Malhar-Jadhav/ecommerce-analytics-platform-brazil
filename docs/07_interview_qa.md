# 07 · Interview / Q&A Bank

Every question someone could reasonably ask about this project, with clear answers, tables and
diagrams. Organized by theme. Use with `05_project_notes_end_to_end.md` (concepts) and
`06_pipeline_designs_and_workarounds.md` (alternatives).

> Tip: for "tell me about your project" use the **60-second pitch** in doc 05 §1, then dive into
> whichever area they probe.

---

## A. Project overview

**A1. What is this project?**
A daily, automated data platform for a Brazilian e-commerce marketplace (Olist). It ingests 9 raw
CSV sources via **ADF**, processes them through a **Medallion** (Bronze→Silver→Gold) lakehouse on
**Azure Databricks + Delta Lake**, builds a **star schema** with **SCD-1/SCD-2**, and serves 15
business questions to BI — with **time travel** and a **data-quality gate**.

**A2. What business problems does it solve?**
Raw multi-source CSVs, no history of seller changes, full reloads (slow), no unified model, slow
reporting. → uniform ingestion, SCD-2 history, incremental loads, star schema, clean Gold tables.

**A3. Why Databricks + Delta and not a plain database?**
Volume + variety + cheap object storage + need for ACID/upsert/time-travel = **lakehouse**. Delta
adds warehouse guarantees (ACID, MERGE, time travel) on top of cheap Parquet on a data lake.

**A4. What was your role / what did you build?**
End-to-end: config-driven Bronze ingestion, 9 Silver cleaning notebooks, Gold dims + fact with
SCD logic, the ADF pipeline + trigger, the Databricks Workflow job, DQ checks, time-travel demos,
15 analytics queries, and full docs.

---

## B. Architecture & Medallion

**B1. Explain the Medallion architecture.**
Three layers, each with one job:
```
🥉 Bronze  raw, as-received, append-only, +ingest_ts, NO transforms
🥈 Silver  clean: cast, trim, dedup, validate, standardize, overwrite
🥇 Gold    business: star schema, SCD, joins, aggregations, BI-ready
```

**B2. Why not just load CSV straight to Gold?**
You'd lose the raw audit trail, mix concerns, and have no trusted intermediate to reprocess from.
Layering isolates failures and lets you fix one layer and rebuild downstream.

**B3. Why is Bronze append-only and "no transforms"?**
Bronze is the **immutable source of truth**. If a cleaning rule is wrong, you fix Silver and
reprocess from Bronze — the raw data is never corrupted. Append keeps every daily snapshot.

**B4. Why is Silver `overwrite` but the fact `append`?**
Silver represents the **current clean state** (rebuilt each run after dedup). The fact is an
**event log** that grows — you append new line items, never rewrite history.

**B5. Bronze vs Silver vs Gold — one-line each.**
Raw truth → trusted/clean → business model. (See table in doc 05 §4.)

---

## C. Data modeling (fact / dimension / star schema)

**C1. What is the grain of your fact table?**
One **order line item** (`order_id` + `order_item_id`) — the finest grain that still has product
and seller, so everything rolls up from it.

**C2. Star vs snowflake — which and why?**
**Star**: dimensions are flat/denormalized for fast, simple BI joins. Snowflake normalizes dims
into sub-tables (more joins, less storage) — not needed here.

**C3. What's a measure vs a dimension?**
Measure = number you aggregate (`price`, `payment_value`, `review_score`, `delivery_days`).
Dimension = descriptive context (customer, product, seller, date, category).

**C4. What is a surrogate key and why use `xxhash64`?**
A synthetic warehouse key independent of source keys. `xxhash64(business_key)` is **deterministic
and stable** across runs (so fact and dims compute the same value), needs no central sequence, and
joins as a fast integer. Trade-off: negligible collision risk vs identity columns.

**C5. What's a degenerate dimension? Example here?**
A dimension attribute stored directly on the fact with no separate dim table — here
`order_status`. It's low-cardinality and order-specific, so no dim table is warranted.

**C6. How do you avoid double-counting revenue at item grain?**
`payment_value` is per-order. Joined naively to items it multiplies. We **allocate** it to each
line by **price share** = `item_price / order_total_price`, so `SUM(payment_value)` = true order
revenue. (`gold_fact_orders.py`.)

**C7. Why is `dim_date` generated, not sourced?**
There's no date source file, but the fact needs a date FK. We generate a gap-free calendar
(2016–2030) with `date_id = yyyyMMdd` to support daily/MoM/seasonal grouping.

---

## D. Slowly Changing Dimensions (SCD)

**D1. Explain SCD types 0/1/2.**
| Type | Behaviour | Example dim |
|------|-----------|-------------|
| 0 | never changes (static) | dim_category |
| 1 | overwrite, no history | dim_customer, dim_product |
| 2 | new row per change + validity dates | dim_seller |

**D2. Why SCD-2 for sellers but SCD-1 for customers?**
Historical **seller location** affects delivery analysis (a seller moved → performance changed),
so we keep history. Customer/product corrections just overwrite — the spec said no history needed.

**D3. Walk me through your SCD-2 logic.**
Columns `effective_from`, `effective_to` (`9999-12-31` for current), `is_current`, version-unique
`seller_sk`. Each run:
1. Find sellers whose `city`/`state` changed vs current version (or are brand new).
2. **Expire** old current row: `is_current=false`, `effective_to=now` (MERGE update).
3. **Insert** the new version as current (append).
Invariant: exactly one `is_current=true` per seller.
```
S1 curitiba   [2023-01-01 → 2024-06-01)  is_current=false
S1 sao paulo  [2024-06-01 → 9999-12-31]  is_current=true
```

**D4. How would you query "what city was seller S1 in on 2023-09-01"?**
```sql
SELECT seller_city FROM gold.dim_seller
WHERE seller_id='S1' AND '2023-09-01' >= effective_from AND '2023-09-01' < effective_to;
```

**D5. Simpler ways to do SCD-2?**
DLT `APPLY CHANGES … STORED AS SCD TYPE 2`, Delta Change Data Feed, or snapshot-append + a
"current" view. (See doc 06 §C.)

---

## E. Delta Lake & time travel

**E1. Delta vs Parquet?**
Delta = Parquet data files + a `_delta_log` transaction log. That log gives **ACID**, **MERGE**,
**time travel**, schema enforcement/evolution — none of which plain Parquet has.

**E2. What is time travel and how do you use it?**
Query/restore previous table versions via the transaction log.
```sql
DESCRIBE HISTORY gold.fact_orders;                       -- versions
SELECT * FROM gold.fact_orders VERSION AS OF 2;          -- by version
SELECT * FROM gold.fact_orders TIMESTAMP AS OF '2026-06-18';
RESTORE TABLE gold.fact_orders TO VERSION AS OF 3;       -- recover bad load
```
Uses: audit, reproducible "as-of" reporting, recovery, version diffs.

**E3. How long can you time travel back?**
Until files are removed by `VACUUM` (default retention 7 days). `VACUUM … RETAIN n HOURS`
controls it; below 7 days needs the safety check disabled.

**E4. What does MERGE do and where do you use it?**
Atomic upsert: `WHEN MATCHED UPDATE / WHEN NOT MATCHED INSERT`. Used in all SCD logic (SCD-1 dims,
SCD-2 expire step).

**E5. OPTIMIZE / Z-Order / VACUUM — what and why?**
`OPTIMIZE` compacts many small files into fewer big ones (faster reads). `ZORDER BY col`
co-locates data for a filter column. `VACUUM` deletes old/unreferenced files.

**E6. What's the small-file problem?**
Many tiny files = slow reads + metadata overhead. Mitigated by `optimizeWrite`, `OPTIMIZE`, and
partitioning sensibly (not over-partitioning).

---

## F. PySpark / coding

**F1. Why explicit schema instead of `inferSchema`?**
Deterministic types, no extra scan pass (faster), and a versioned source contract. Bad values go
to `_rescued_data` instead of corrupting types.

**F2. How do you dedup to the latest record?**
```python
w = Window.partitionBy(keys).orderBy(F.col("ingest_ts").desc())
df.withColumn("rn", F.row_number().over(w)).filter("rn=1").drop("rn")
```
`dropDuplicates` keeps an arbitrary row; this keeps the **latest**.

**F3. How is the pipeline made reusable/DRY?**
A shared `common_functions` notebook (`read_csv`, `write_delta`, `deduplicate`,
`drop_null_keys`, `validate_schema`, `clean_money`, …) `%run` into every notebook; one
parametrised Bronze notebook handles all 9 sources via a widget + config registry.

**F4. `append` vs `overwrite` vs `merge` — when each?**
Append = Bronze + fact (immutable/growing). Overwrite = Silver + SCD-1/static dims (current
state). Merge = upsert for SCD logic.

**F5. How do you handle nulls and bad numbers?**
`drop_null_keys` for PKs; `fillna` defaults (e.g. `review_score=0`); `clean_money` strips `$`/junk
then casts; out-of-range values nulled or quarantined to `*_errors`.

**F6. What's a narrow vs wide transformation? (general Spark)**
Narrow = no shuffle (`map`, `filter`). Wide = shuffle across partitions (`groupBy`, `join`,
`window`). Wide ops are the expensive ones.

**F7. How would you optimize a slow join?**
Broadcast the small side (`broadcast(df)`), filter early, partition/Z-Order on join keys, avoid
skew (salting), cache reused DataFrames.

---

## G. SQL

**G1. How do you compute MoM revenue growth?**
`LAG()` window over monthly totals:
```sql
WITH m AS (SELECT year_month, SUM(payment_value) rev FROM ... GROUP BY year_month)
SELECT year_month, rev, LAG(rev) OVER (ORDER BY year_month) prev,
       100*(rev-LAG(rev) OVER (ORDER BY year_month))/LAG(rev) OVER (ORDER BY year_month) AS growth_pct
FROM m;
```

**G2. Top-N sellers per state?** `ROW_NUMBER()/RANK() OVER (PARTITION BY state ORDER BY rev DESC)` then filter `<=N`.

**G3. Repeat-customer rate?** Count customers with `>1` distinct order ÷ total customers (CTE in scenario #10).

**G4. WHERE vs HAVING?** WHERE filters rows before aggregation; HAVING filters after `GROUP BY`.

**G5. Why filter `review_score > 0` in rating queries?** `0` means "no/invalid review" (our default); including it skews averages down.

---

## H. ADF / orchestration

**H1. What does ADF do here?**
Copies the 9 source CSVs into ADLS `landing/`, then triggers the Databricks notebooks in order,
on a daily 07:00 schedule. It's the ingestion + orchestration layer.

**H2. Key ADF objects?**
| Object | Role |
|--------|------|
| Linked service | connection to a system (blob, ADLS, Databricks, Key Vault) |
| Dataset | shape/location of data (parameterized CSV) |
| Pipeline | the activities (ForEach Copy + Databricks notebooks) |
| Trigger | schedule (daily 07:00) |
| Integration runtime | compute that moves data |

**H3. How are secrets handled?**
Azure **Key Vault** linked service; ADF/Databricks authenticate with **managed identity** (MSI).
No credentials in code/config.

**H4. ADF vs Databricks Workflows — when which?**
ADF: many source connectors, enterprise ingestion standard. Workflows: one-platform, native task
DAG, cheaper. Both are provided. (Doc 06 P1 vs P2.)

**H5. How do you pass parameters from ADF to a notebook?**
`baseParameters` on the `DatabricksNotebook` activity → `dbutils.widgets.get(...)` in the notebook
(e.g. `run_date`, `dataset`).

---

## I. Incremental, idempotency, performance

**I1. How is the pipeline incremental?**
Bronze appends daily files; Silver dedups to latest-per-key; dims MERGE; fact appends only new
natural keys (`left_anti`). No full reload of Gold.

**I2. What is idempotency and how do you guarantee it?**
Re-running a day yields the same result. Dims use MERGE; the fact anti-joins existing keys before
appending, so duplicates can't form.

**I3. How do you partition the fact and why?**
By `order_year_month`. Date-range queries (most BI) then scan only relevant months (partition
pruning) instead of the whole table.

**I4. How would this scale to 100× data?**
Auto Loader for incremental files, partition + Z-Order, autoscaling clusters, avoid wide shuffles,
liquid clustering instead of static partitions, photon engine.

---

## J. Data quality & reliability

**J1. What data-quality checks run?**
Row-count>0, null PK/SK, duplicate/grain uniqueness, one-current SCD-2 row, schema validation,
referential integrity (orphans). Results → `gold.dq_results`; job **fails on FATAL**.

**J2. Where do bad records go?**
Quarantined to `*_errors` tables (e.g. `silver.order_items_errors`) — dead-letter pattern, nothing
silently dropped.

**J3. A daily load corrupted Gold — what do you do?**
Time-travel `RESTORE TABLE … TO VERSION AS OF <good>`; inspect `DESCRIBE HISTORY`; fix the bug;
reprocess from Bronze/Silver (still intact).

**J4. How do you monitor failures?**
ADF activity retries + alerts; Databricks job `email_notifications.on_failure`; DQ table trend.

---

## K. Scenario / "what if" questions

**K1. A new source column appears — what happens?**
`mergeSchema=true` lets Bronze/Silver absorb it without failing; you then map it forward when ready.

**K2. The same order arrives in two daily files — duplicates?**
No. Bronze keeps both (audit); Silver dedups to latest by `ingest_ts`; fact anti-joins on natural
key. Final Gold has one row.

**K3. A seller changes state twice in a month?**
SCD-2 creates two new versions with consecutive validity windows; all kept, one current.

**K4. How do you backfill history?**
Replay historical files through Bronze→Gold; fact append is idempotent; SCD-2 builds versions in
file order (pass a logical `effective_from` if you need true historical dates).

**K5. How would you add a new dimension (e.g. dim_payment)?**
New Silver clean step → new `gold_dim_payment` (pick SCD type) → add FK in fact build → add DQ
checks → wire into `run_gold_all`.

**K6. ZIP codes lost leading zeros — why, and fix?**
We cast ZIP to INT per spec; Brazilian prefixes can start with 0. Fix: store as zero-padded STRING
(flagged in `docs/04_pdf_improvements.md`).

---

## L. Trade-offs & "why did you…" (defend your design)

**L1. Why xxhash64 over identity columns?** Deterministic & stable across runs, no single-writer
coordination. Trade-off: tiny collision risk.

**L2. Why append+dedup instead of MERGE into Bronze?** Bronze must be the raw audit log; dedup is a
*Silver* concern. Keeps Bronze faithful.

**L3. Why allocate payments by price-share instead of a separate payment fact?** Simpler single
fact for the spec's questions; documented approximation. A dedicated `fact_payments` is the
"correct" multi-fact alternative.

**L4. Why current seller_sk in the fact, not point-in-time?** Orders are processed near event time,
so current ≈ correct; documented. True point-in-time would join on the SCD-2 validity window.

**L5. Why two orchestrators (ADF + Workflows)?** Spec mentions both; ADF for ingestion realism,
Workflows as the one-platform alternative.

---

## M. Rapid-fire definitions

| Term | One-liner |
|------|-----------|
| Lakehouse | data lake + warehouse features via Delta |
| Medallion | Bronze/Silver/Gold layered design |
| Grain | what one fact row means |
| Surrogate key | synthetic warehouse key (xxhash64) |
| SCD-1 / SCD-2 | overwrite / keep history |
| Upsert / MERGE | insert-or-update atomically |
| Idempotent | re-run → same result |
| Time travel | read/restore old Delta version |
| Partition pruning | skip irrelevant partitions at read |
| Dead-letter / quarantine | table of rejected rows |
| Degenerate dimension | dim attribute stored on the fact |
| ACID | atomic/consistent/isolated/durable writes |

---

## N. Likely "gotcha" questions (and crisp answers)

- **"Is Bronze deduped?"** No — dedup is Silver's job; Bronze keeps everything.
- **"Does overwrite lose history?"** For Silver/SCD-1 yes (by design); the *fact* never overwrites,
  and Delta **time travel** still retains prior versions until VACUUM.
- **"What if the DQ job fails?"** It raises and stops the run before BI sees bad data — that's the
  point; investigate `gold.dq_results`.
- **"Streaming or batch?"** Batch (daily). Auto Loader/DLT would make it micro-batch/streaming.
- **"Unity Catalog required?"** Used here (`ecommerce` catalog); on Hive metastore drop the catalog
  prefix (`bronze.x` instead of `ecommerce.bronze.x`).
```

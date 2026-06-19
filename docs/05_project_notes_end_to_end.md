# 05 · End-to-End Project Notes (Study Guide)

Everything about this project in one place — concepts, architecture, data model, and every
design decision — written so you can **explain the whole project to anyone** without re-reading
the code. Pair with `06_pipeline_designs_and_workarounds.md` and `07_interview_qa.md`.

---

## 1. The 60-second pitch

> *"I built a fully automated daily data platform for a Brazilian e-commerce marketplace
> (Olist). Raw CSVs from 9 source systems land in cloud storage; **Azure Data Factory** copies
> them and triggers **Azure Databricks** notebooks that process the data through a **Medallion
> architecture** (Bronze → Silver → Gold) on **Delta Lake**. The Gold layer is a **star schema**
> with a fact table and dimensions, including **SCD-1** and **SCD-2** history tracking. The whole
> thing runs at 7 AM daily, is incremental, has a data-quality gate, and supports **time travel**
> for audit and recovery. BI tools query the Gold layer to answer 15 business questions."*

---

## 2. Business problem → solution map

| Business pain (from spec) | How the project solves it |
|---------------------------|---------------------------|
| Data comes as raw CSVs from many systems | ADF copies all 9 sources; Bronze ingests them uniformly |
| No historical tracking of seller changes | **SCD-2** `dim_seller` keeps full location history |
| Full reload every time (slow) | **Incremental**: dedup-latest in Silver, append-only fact, MERGE dims |
| No unified analytics model | **Star schema** (`fact_orders` + 6 dimensions) |
| Reporting slow & unreliable | Clean Gold Delta tables, partitioned + `OPTIMIZE`d, DQ-gated |
| Manual, error-prone | Fully automated daily pipeline (ADF trigger / Databricks job at 07:00) |

**15 business questions answered** (daily revenue, MoM growth, top sellers, category revenue,
weak categories, late-delivery sellers, delivery→rating impact, city/state revenue, repeat-rate,
payment patterns, revenue per customer, seller risk, seasonal sales, product performance,
seller-over-time via SCD-2). Queries live in `sql/analytics/business_scenarios.sql`.

---

## 3. The dataset (Olist Brazilian e-commerce)

9 source files, ~100K orders. Relationships:

```
                         ┌───────────────┐
                         │   customers   │ customer_id
                         └──────┬────────┘
                                │ 1
                                │
                                ▼ *
   reviews *───1 ┌──────────────────────────┐ 1───* payments
   (order_id)    │          orders          │       (order_id)
                 │  order_id (PK)           │
                 │  customer_id (FK)        │
                 └──────────┬───────────────┘
                            │ 1
                            ▼ *
                  ┌───────────────────────┐
                  │      order_items       │  (order_id, order_item_id) PK
                  │  product_id (FK)       │
                  │  seller_id  (FK)       │
                  └───┬───────────────┬────┘
                      │ *             │ *
                      ▼ 1             ▼ 1
              ┌────────────┐    ┌────────────┐
              │  products  │    │  sellers   │
              │ product_id │    │ seller_id  │
              └─────┬──────┘    └────────────┘
                    │ * (category name, Portuguese)
                    ▼ 1
        ┌────────────────────────┐         ┌──────────────┐
        │  category_translation  │         │ geolocation  │ (zip prefix → lat/lng)
        │  PT → EN               │         └──────────────┘
        └────────────────────────┘
```

| File | Grain | Role in Gold |
|------|-------|--------------|
| olist_customers_dataset | one customer record | dim_customer |
| olist_orders_dataset | one order | fact (header) |
| olist_order_items_dataset | one line item | **fact grain** |
| olist_order_payments_dataset | one payment per order | fact (aggregated) |
| olist_order_reviews_dataset | one review | fact (aggregated) |
| olist_products_dataset | one product | dim_product |
| olist_sellers_dataset | one seller | dim_seller (SCD-2) |
| olist_geolocation_dataset | many rows per zip | dim_geolocation |
| product_category_name_translation | PT→EN lookup | dim_category |

---

## 4. Medallion architecture — the core idea

> **Why medallion?** Separate *concerns* into layers so each layer has one job, failures are
> isolated, and you can always reprocess downstream from a trusted upstream layer.

```
   RAW CSV ──► 🥉 BRONZE ───────► 🥈 SILVER ───────► 🥇 GOLD ───────► BI
              (as-received)     (clean & trusted)   (business model)
              append-only       dedup, cast,        star schema,
              +ingest_ts        validate, std       SCD-1/2, fact
              NO transforms      overwrite           MERGE/append
```

| | Bronze | Silver | Gold |
|--|--------|--------|------|
| **Purpose** | Faithful copy of source | Clean, standardized, validated | Business-ready analytics |
| **Transforms** | None (only add `ingest_ts`) | Cast, trim, lowercase, dedup, range checks | Joins, aggregations, SCD, surrogate keys |
| **Write mode** | `append` | `overwrite` | dims: `MERGE`/`overwrite`; fact: `append` |
| **Schema** | Explicit, source-shaped | Cleaned, typed | Star schema |
| **Who reads it** | Silver | Gold | BI / analysts |
| **Can be rebuilt from** | source files | Bronze | Silver |

**Key principle:** *never transform in Bronze*. If a cleaning rule is wrong, you fix Silver and
reprocess — Bronze (the raw truth) is untouched.

---

## 5. Gold star schema (dimensional model)

```
                         ┌─────────────┐
                         │  dim_date   │
                         │ date_id PK  │
                         └──────┬──────┘
                                │
  ┌──────────────┐       ┌──────┴────────┐       ┌──────────────┐
  │ dim_customer │◄──────│  fact_orders  │──────►│ dim_product  │
  │ customer_sk  │       │  (grain =     │       │ product_sk   │
  └──────────────┘       │  order line   │       └──────┬───────┘
                         │  item)        │              │
  ┌──────────────┐       │  measures:    │       ┌──────┴───────┐
  │  dim_seller  │◄──────│  price        │       │ dim_category │
  │ seller_sk    │       │  freight      │       └──────────────┘
  │ (SCD-2)      │       │  payment_value│
  └──────────────┘       │  review_score │       ┌──────────────┐
                         │  delivery_days│──────►│dim_geolocation│
                         └───────────────┘       └──────────────┘
```

- **Fact (`fact_orders`)** — *measures* (numbers you aggregate) + *foreign keys* to dimensions.
  Grain = one **order line item** (`order_id` + `order_item_id`).
- **Dimensions** — descriptive context (who/what/where/when).
- **Star** (not snowflake): dimensions are denormalized/flat for fast BI joins.

### Why this grain?
Order-line-item is the **finest grain** that still has product *and* seller. From it you can roll
up to order, seller, product, customer, category, date, city/state — anything. Coarser grain
(per order) would lose product/seller detail.

### Surrogate keys
`*_sk = xxhash64(business_key)` — a stable integer key. Why: BI joins on integers are faster,
and surrogate keys insulate the warehouse from messy/changing source keys. They're deterministic
so the fact and dims compute the same value independently.

---

## 6. Slowly Changing Dimensions (SCD) — the heart of "history"

| SCD type | Meaning | Used for | Behaviour |
|----------|---------|----------|-----------|
| **SCD-0** | Never changes | dim_category | Static lookup, full overwrite |
| **SCD-1** | Overwrite, no history | dim_customer, dim_product | Update row in place (latest wins) |
| **SCD-2** | Keep full history | dim_seller | New row per change + validity dates |

### SCD-1 (example: customer moves city)
```
Before:  customer_sk=hash(C1) | C1 | sao paulo
After :  customer_sk=hash(C1) | C1 | rio de janeiro    ← overwritten, old value lost
```
Implemented with Delta `MERGE … WHEN MATCHED UPDATE ALL … WHEN NOT MATCHED INSERT ALL`.

### SCD-2 (example: seller moves city) — **the showcase feature**
```
seller_id  city        effective_from  effective_to        is_current
S1         curitiba    2023-01-01      2024-06-01          false   ← old version kept
S1         sao paulo   2024-06-01      9999-12-31          true    ← new current version
```
Why SCD-2 for sellers? *Historical seller location matters* — if a seller moved and delivery
performance changed, you must attribute past orders to the *location at that time*.

**Algorithm (2-step Delta MERGE):**
1. Detect rows whose `seller_city`/`seller_state` differ from the current version.
2. **Expire** the old current row (`is_current=false`, set `effective_to=now`).
3. **Insert** the new version as current.
Invariant: exactly **one** `is_current=true` row per seller.

---

## 7. Incremental processing (no full reload)

| Layer | Incremental technique | Why |
|-------|----------------------|-----|
| Bronze | `append` every daily file | keep full raw history cheaply |
| Silver | `deduplicate(..., order_col="ingest_ts")` → keep latest per key, `overwrite` | collapse repeated keys to current truth |
| Dims (SCD-1) | `MERGE` upsert | only changed/new rows touched |
| Dim (SCD-2) | expire + insert only changed | history preserved, minimal writes |
| Fact | `left_anti` on natural key → append only new line items | idempotent; re-runs don't duplicate |

**Idempotency** = running the same day twice produces the same result. Achieved via MERGE (dims)
and anti-join-before-append (fact).

---

## 8. Delta Lake — what it gives us

| Feature | What it is | Where used |
|---------|-----------|-----------|
| **ACID transactions** | safe concurrent reads/writes, no half-written tables | every write |
| **Time travel** | query/restore old versions (`VERSION/TIMESTAMP AS OF`, `RESTORE`) | `06_time_travel` |
| **MERGE (upsert)** | insert+update in one atomic op | all SCD logic |
| **Schema evolution** | `mergeSchema` absorbs new columns | Bronze/Silver writes |
| **OPTIMIZE / Z-Order** | compact small files for speed | fact after load |
| **VACUUM** | delete old files, bounds time-travel window | maintenance |
| **DESCRIBE HISTORY** | audit log of every commit | Bronze + demos |

**Why Delta over plain Parquet?** Parquet has no ACID, no upsert, no time travel, no
transaction log — you can't safely MERGE or roll back. Delta = Parquet + `_delta_log`.

---

## 9. The daily run, step by step (07:00)

```
1. TRIGGER tr_daily_7am fires (or Databricks job cron 0 0 7)
2. ADF ForEach → Copy 9 CSVs  → ADLS landing/olist/
3. Databricks: 00_create_catalog_schemas   (idempotent)
4. Databricks: run_bronze_all → bronze_ingestion ×9   (append + ingest_ts)
5. Databricks: run_silver_all → silver_* ×9           (clean, overwrite)
6. Databricks: run_gold_all   → gold_dim_* ×6 then gold_fact_orders
7. Databricks: data_quality_checks  → gold.dq_results; FAIL job on FATAL
8. BI / SQL queries the Gold star schema
```
Orchestration: **ADF** is primary (spec: "ingestion with ADF"); a **Databricks Workflows** job
JSON is provided as an alternative (spec lists "Databricks Workflows").

---

## 10. Data quality gate

Final task runs `data_quality_checks.py`:
1. **Row count > 0** for every Gold table.
2. **No null** PK/surrogate keys.
3. **No duplicate** business/grain keys; **one current** SCD-2 row per seller.
4. **Schema validation** — required fact columns present.
5. **Referential integrity** — no orphan FKs (WARN).

Results appended to `ecommerce.gold.dq_results`; job **raises an exception on any FATAL failure**,
so bad data never reaches BI. This is the "clean & trusted" guarantee.

---

## 11. Key design decisions & trade-offs (be ready to defend these)

| Decision | Why | Trade-off / alternative |
|----------|-----|-------------------------|
| Read Bronze with **explicit schema** | deterministic, fast, no bad casts | must maintain schema in `config` (vs `inferSchema`) |
| Bronze **append**, dedup in Silver | keeps raw audit trail | storage grows (mitigate with VACUUM/retention) |
| **xxhash64** surrogate keys | stable, deterministic, no coordination | tiny collision risk (vs identity columns) |
| **Payment allocated by price-share** | avoids revenue double-count at item grain | approximation (vs separate payment fact) |
| Fact joins **current** seller_sk | simple; orders built near event time | not point-in-time SCD-2 join (documented) |
| **SCD-2 only for sellers** | only place history was required | customers/products lose history (spec said so) |
| **ADF + Databricks** split | ADF=ingestion/orchestration, Databricks=compute | could do all-in-Databricks (see doc 06) |
| `left_anti` append for fact | idempotent re-runs | extra read of existing keys (vs MERGE) |
| ZIP as **INT** (per spec) | matches spec | drops leading zeros (flagged; STRING is safer) |

---

## 12. Glossary (quick reference)

- **Medallion** — Bronze/Silver/Gold layered lakehouse design.
- **Lakehouse** — data lake (cheap storage) + warehouse features (ACID, schema) via Delta.
- **Grain** — what one fact row represents.
- **Fact / Dimension** — measures vs descriptive context.
- **Surrogate key** — synthetic warehouse key (here `xxhash64`).
- **SCD** — Slowly Changing Dimension (0/1/2 = static/overwrite/history).
- **Upsert / MERGE** — insert if new, update if exists.
- **Idempotent** — re-running yields the same result.
- **Time travel** — read/restore a previous Delta version.
- **Degenerate dimension** — a dimension attribute stored on the fact (e.g. `order_status`).
- **Quarantine / dead-letter** — table holding rejected rows (`*_errors`).
- **DQ gate** — checks that block bad data from advancing.
- **Partitioning** — physically splitting data by a column (here `order_year_month`) to scan less.
```

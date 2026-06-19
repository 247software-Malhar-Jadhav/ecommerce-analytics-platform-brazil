# 01 · Architecture & High-Level Design (HLD)

**Project:** E-Commerce Analytics Platform (Brazil / Olist)
**Pattern:** Azure Databricks Lakehouse · Medallion (Bronze → Silver → Gold)
**Author:** Malhar Jadhav · Based on the spec by Rahul M

---

## 1. Business Context

A large Brazilian e-commerce marketplace receives daily CSV extracts from multiple source
systems (customers, orders, items, payments, reviews, products, sellers, geolocation,
category translation). The business needs a **fully automated daily platform** that turns
raw, messy CSVs into clean, trusted, BI-ready analytics — answering questions such as daily
revenue, top sellers, best categories, delivery performance, review satisfaction, and seller
location history.

### Key requirements
- Fully automated daily pipeline (runs at **07:00**, America/São_Paulo)
- Clean & trusted data with data-quality gates
- **Incremental** processing (no full reload each day)
- **SCD-1** (customer, product) and **SCD-2** (seller) implemented
- **Star schema** (fact + dimensions)
- BI-ready Gold tables + **Delta time travel** for audit/recovery

---

## 2. High-Level Architecture

```
                              ┌──────────────────────────────────────────────────────────┐
                              │                  AZURE DATA FACTORY (ADF)                  │
                              │   pl_ecommerce_medallion_daily  ·  trigger tr_daily_7am    │
   SOURCE DROP                │                                                            │
 ┌───────────────┐  Copy      │  ForEach ──► Copy 9 CSVs ──► ADLS 'landing/olist'          │
 │ 9 Olist CSVs  │──activity──►│                  │                                         │
 │ (blob / SFTP) │            │                  ▼  (Databricks Notebook activities)       │
 └───────────────┘            │   setup ► run_bronze ► run_silver ► run_gold ► data_quality│
                              └────────────────────────────┬─────────────────────────────┘
                                                           │ invokes
                                                           ▼
   ┌──────────────────────────────  AZURE DATABRICKS (Apache Spark + Delta Lake)  ──────────────────────────────┐
   │                                                                                                            │
   │   BRONZE (raw)              SILVER (clean/standardized)            GOLD (business / star schema)            │
   │   ───────────────           ──────────────────────────            ──────────────────────────────          │
   │   ecommerce.bronze.*        ecommerce.silver.*                     ecommerce.gold.*                         │
   │   • append-only             • dedup latest-per-key                 • dim_customer  (SCD-1)                  │
   │   • +ingest_ts              • cast types, trim, lowercase          • dim_product   (SCD-1)                  │
   │   • no transforms           • $ / special-char cleanup             • dim_seller    (SCD-2)                  │
   │   • Delta                   • range validation + error tables      • dim_category  (SCD-0)                  │
   │                             • +updated_ts · overwrite              • dim_date / dim_geolocation             │
   │                                                                    • fact_orders (append, partitioned)      │
   └──────────────────────────────────────────────────────────────────────────────┬─────────────────────────┘
                                                                                    │
                                                                                    ▼
                                                              ┌──────────────────────────────────┐
                                                              │  SERVE: Power BI / SQL / Azure ML │
                                                              │  15 business-scenario queries     │
                                                              └──────────────────────────────────┘
```

---

## 3. Technology Stack

| Layer            | Technology                                   |
|------------------|----------------------------------------------|
| Cloud platform   | Azure                                        |
| Ingestion / orchestration | **Azure Data Factory** (copy + trigger) |
| Compute engine   | **Apache Spark** on Azure Databricks         |
| Language         | **Python / PySpark** + **Spark SQL**         |
| Storage format   | **Delta Lake** (ACID, time travel)           |
| Lake storage     | Azure Data Lake Storage Gen2                 |
| Governance       | Unity Catalog (`ecommerce` catalog)          |
| Alt. orchestration | Databricks Workflows (job JSON included)   |
| Serving          | Power BI / Databricks SQL                    |
| Secrets          | Azure Key Vault                              |

---

## 4. Medallion Layers — responsibilities

### Bronze — raw ingestion
- Store source data **exactly as received**; the only additions are `ingest_ts` + `source_file`.
- Explicit schema read (no inference); malformed rows captured in `_rescued_data`.
- **Append** mode — Bronze becomes the immutable daily audit log.
- No dedup, no filter, no join, no rename.

### Silver — clean & standardize
- Read Bronze Delta, deduplicate to the **latest row per business key** (incremental-friendly).
- Cast data types, trim strings, lowercase/uppercase per spec, strip `$` and special chars.
- Validate numeric ranges; quarantine invalid rows into `*_errors` tables.
- Add `updated_ts`; write with **overwrite** mode (clean current-state snapshot).

### Gold — business / star schema
- Build conformed **dimensions** and a central **fact**.
- Apply **SCD-1** (overwrite via MERGE) and **SCD-2** (history via expire+insert MERGE).
- `fact_orders` at order-line-item grain; **append-only**, partitioned by `order_year_month`.
- Optimized for analytics; `OPTIMIZE` + data-quality gate before BI consumption.

---

## 5. Data Flow (daily, 07:00)

1. **ADF Copy** — 9 source CSVs → ADLS `landing/olist/`.
2. **Setup** — ensure catalog/schemas exist (idempotent).
3. **Bronze** — append each raw file to `ecommerce.bronze.<table>` with `ingest_ts`.
4. **Silver** — clean/standardize each table → `ecommerce.silver.<table>`.
5. **Gold** — build dimensions (SCD logic) then `fact_orders`.
6. **Data Quality** — row-count, null-PK, duplicate, schema and referential checks;
   results to `ecommerce.gold.dq_results`; the job **fails** on any FATAL violation.
7. **Serve** — Power BI / SQL run the 15 business-scenario queries.

---

## 6. Incremental & Idempotency Strategy

| Table type        | Load strategy                                                       |
|-------------------|---------------------------------------------------------------------|
| Bronze            | Append every daily file (full history retained)                     |
| Silver            | Dedup latest-per-key on `ingest_ts`, overwrite current snapshot     |
| SCD-1 dims        | Delta `MERGE` upsert (stable surrogate keys, attributes overwritten)|
| SCD-2 dim_seller  | Expire changed current rows, insert new versions (no data lost)     |
| fact_orders       | `left_anti` on natural key → append only new line items (idempotent)|

---

## 7. Reliability & Recovery

- **ACID** writes via Delta.
- **Time travel** (`VERSION/TIMESTAMP AS OF`, `RESTORE`) for audit and bad-load recovery —
  see `notebooks/06_time_travel/time_travel_demo` and `sql/time_travel/`.
- **Data-quality gate** stops bad data reaching Gold/BI.
- **Retries** on ADF copy + notebook activities; failure email via Databricks job.

See `docs/02_lld.md` for table-level detail and `docs/03_code_flow_and_file_paths.md` for the
file-by-file execution map.

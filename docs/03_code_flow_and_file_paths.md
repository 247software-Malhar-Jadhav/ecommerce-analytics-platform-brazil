# 03 · Code Flow & File-Path Directory

A file-by-file map of the repository and the exact order in which code executes during a
daily run. Use this as the navigation index for the codebase.

> **How to read the code:** every `.py` notebook and `.sql` file is **richly commented inline**.
> Markdown (`# MAGIC %md`) cells introduce each section, plain `#`/`--` comments explain the
> *what* and *why* of individual lines, and the helper functions in
> `notebooks/04_utils/common_functions.py` carry docstrings. Suggested reading order for a
> newcomer: `04_utils/common_functions.py` → `config/schema_definitions.py` →
> `01_bronze/bronze_ingestion.py` → a couple of `02_silver/silver_*.py` →
> `03_gold/gold_dim_seller_scd2.py` (SCD-2) and `03_gold/gold_fact_orders.py` (fact) →
> `05_quality/data_quality_checks.py` → `06_time_travel/time_travel_demo.py`.

---

## 1. Repository tree

```
ecommerce-analytics-platform-brazil/
├── README.md                              # setup + run guide
├── requirements.txt                       # local dev deps (pyspark, delta-spark, pytest)
├── .gitignore
│
├── config/
│   ├── pipeline_config.json               # catalog, ADLS paths, source registry, 7AM schedule
│   └── schema_definitions.py              # explicit StructType schema for all 9 sources
│
├── notebooks/
│   ├── 00_setup/
│   │   ├── 00_create_catalog_schemas.py   # create catalog + bronze/silver/gold schemas
│   │   ├── run_bronze_all.py              # orchestration: loop all datasets -> bronze
│   │   ├── run_silver_all.py              # orchestration: run all silver notebooks
│   │   └── run_gold_all.py               # orchestration: dims then fact
│   ├── 01_bronze/
│   │   └── bronze_ingestion.py            # parametrised raw ingest (widget: dataset)
│   ├── 02_silver/
│   │   ├── silver_customers.py
│   │   ├── silver_orders.py
│   │   ├── silver_order_items.py
│   │   ├── silver_payments.py
│   │   ├── silver_reviews.py
│   │   ├── silver_products.py
│   │   ├── silver_sellers.py
│   │   ├── silver_geolocation.py
│   │   └── silver_category_translation.py
│   ├── 03_gold/
│   │   ├── gold_dim_customer.py           # SCD-1 MERGE
│   │   ├── gold_dim_product.py            # SCD-1 MERGE + category join
│   │   ├── gold_dim_seller_scd2.py        # SCD-2 expire/insert
│   │   ├── gold_dim_category.py           # SCD-0 static
│   │   ├── gold_dim_date.py               # derived calendar
│   │   ├── gold_dim_geolocation.py        # optional dim
│   │   └── gold_fact_orders.py            # star-schema fact (append, partitioned)
│   ├── 04_utils/
│   │   └── common_functions.py            # shared read/write/dedup/null/validate helpers
│   ├── 05_quality/
│   │   └── data_quality_checks.py         # DQ gate -> gold.dq_results, fail on FATAL
│   └── 06_time_travel/
│       └── time_travel_demo.py            # Delta time-travel demo & audit
│
├── sql/
│   ├── ddl/gold_star_schema.sql           # reference DDL for gold dims + fact
│   ├── analytics/business_scenarios.sql   # 15 business-scenario queries
│   └── time_travel/time_travel_queries.sql# version/timestamp/restore/vacuum
│
├── adf/
│   ├── linkedServices/                    # ls_source_blob, ls_adls_gen2, ls_azure_databricks, ls_keyvault
│   ├── datasets/                          # ds_source_csv, ds_landing_adls
│   ├── pipeline/pl_ecommerce_medallion_daily.json
│   ├── trigger/tr_daily_7am.json
│   └── arm_template/                      # ARMTemplateForFactory.json + parameters
│
├── databricks/
│   └── workflows/job_ecommerce_medallion.json   # Databricks Workflows alternative orchestrator
│
└── docs/
    ├── 01_architecture_hld.md
    ├── 02_lld.md
    ├── 03_code_flow_and_file_paths.md     # (this file)
    └── 04_pdf_improvements.md
```

---

## 2. Execution order (daily run at 07:00)

```
ADF trigger tr_daily_7am
   └─► pipeline pl_ecommerce_medallion_daily
        1. ForEach_CopyToLanding ── Copy_CSV_to_Landing  (9× CSV → ADLS landing/olist)
        2. Setup_Catalog_Schemas ─► notebooks/00_setup/00_create_catalog_schemas.py
        3. Run_Bronze ───────────► notebooks/00_setup/run_bronze_all.py
                                       └─ loops → notebooks/01_bronze/bronze_ingestion.py (×9)
        4. Run_Silver ───────────► notebooks/00_setup/run_silver_all.py
                                       └─ runs → notebooks/02_silver/silver_*.py (×9)
        5. Run_Gold ─────────────► notebooks/00_setup/run_gold_all.py
                                       ├─ dims → notebooks/03_gold/gold_dim_*.py (×6)
                                       └─ fact → notebooks/03_gold/gold_fact_orders.py
        6. Run_DataQuality ──────► notebooks/05_quality/data_quality_checks.py
```

Every notebook starts with `%run ../04_utils/common_functions` to import the shared helpers
(and Bronze additionally `%run ../../config/schema_definitions`).

---

## 3. Dependency / data-lineage flow

```
landing/olist/*.csv
      │  (bronze_ingestion, append + ingest_ts)
      ▼
ecommerce.bronze.{customers, orders, order_items, payments, reviews,
                  products, sellers, geolocation, category_translation}
      │  (silver_*, clean/standardize/validate, overwrite)
      ▼
ecommerce.silver.{same 9 tables}  (+ silver.order_items_errors)
      │  (gold_*, dims first, then fact)
      ▼
ecommerce.gold:
   dim_customer ◄─ silver.customers
   dim_product  ◄─ silver.products ⋈ silver.category_translation
   dim_seller   ◄─ silver.sellers              (SCD-2)
   dim_category ◄─ silver.category_translation
   dim_date     ◄─ generated
   dim_geolocation ◄─ silver.geolocation
   fact_orders  ◄─ silver.order_items ⋈ silver.orders ⋈ silver.payments
                   ⋈ silver.reviews ⋈ gold.dim_seller(current)
      │
      ▼
gold.dq_results (data-quality audit)   →   Power BI / SQL (15 scenarios)
```

---

## 4. Where each requirement is implemented

| Requirement | File(s) |
|-------------|---------|
| ADF end-to-end ingestion | `adf/pipeline/pl_ecommerce_medallion_daily.json`, `adf/datasets/*`, `adf/linkedServices/*`, `adf/trigger/tr_daily_7am.json` |
| Bronze raw ingest + ingest_ts | `notebooks/01_bronze/bronze_ingestion.py` |
| Data cleaning / Silver | `notebooks/02_silver/silver_*.py` |
| Gold star schema | `notebooks/03_gold/gold_*.py`, `sql/ddl/gold_star_schema.sql` |
| SCD-1 | `gold_dim_customer.py`, `gold_dim_product.py` |
| SCD-2 | `gold_dim_seller_scd2.py` |
| Incremental processing | dedup-latest in Silver; `left_anti` append in fact; MERGE in dims |
| Time travel | `notebooks/06_time_travel/time_travel_demo.py`, `sql/time_travel/time_travel_queries.sql` |
| Utility functions | `notebooks/04_utils/common_functions.py` |
| Data quality / unit tests | `notebooks/05_quality/data_quality_checks.py` |
| Python / PySpark | all `notebooks/**/*.py` |
| SQL | `sql/**/*.sql`, `%sql` cells in notebooks |
| Orchestration (Databricks Workflows) | `databricks/workflows/job_ecommerce_medallion.json` |
| 15 business scenarios | `sql/analytics/business_scenarios.sql` |

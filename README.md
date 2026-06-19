# 🛒 E-Commerce Analytics Platform (Brazil) — Medallion Lakehouse

An end-to-end, production-style **data engineering pipeline** for the Brazilian
e-commerce (Olist) dataset, built on the **Azure Databricks Medallion architecture**
(Bronze → Silver → Gold) with **Azure Data Factory** ingestion, **Delta Lake** storage,
**SCD-1 / SCD-2**, a **star schema**, **Delta time travel**, and a **data-quality gate**.

> Stack: Azure Data Factory · Azure Databricks · Apache Spark · PySpark · Spark SQL ·
> Delta Lake · ADLS Gen2 · Unity Catalog · Databricks Workflows

---

## 📌 What it does

Raw daily CSVs → cleaned, conformed, BI-ready star schema answering 15 business questions
(daily revenue, top sellers, best categories, delivery performance, review satisfaction,
seller location history, customer retention, seasonal trends, …).

| Layer | Storage | Purpose |
|-------|---------|---------|
| 🥉 Bronze | `ecommerce.bronze.*` | Raw, append-only, `ingest_ts` added, no transforms |
| 🥈 Silver | `ecommerce.silver.*` | Cleaned, standardized, validated, deduped |
| 🥇 Gold | `ecommerce.gold.*` | Star schema: `fact_orders` + `dim_customer/product/seller/category/date/geolocation` |

📖 Read the docs in [`docs/`](docs/): **[HLD](docs/01_architecture_hld.md)** ·
**[LLD](docs/02_lld.md)** · **[Code flow & file paths](docs/03_code_flow_and_file_paths.md)** ·
**[Spec improvements](docs/04_pdf_improvements.md)**.

> 🧑‍🏫 **Learning-friendly:** every notebook and SQL file is **heavily commented inline** —
> each block explains *what* it does and *why* (the data-engineering reasoning). Read the code
> top-to-bottom alongside the LLD and you can follow the full Bronze → Silver → Gold logic
> without prior context.

---

## 🗂 Repository structure

```
config/        pipeline_config.json, schema_definitions.py
notebooks/     00_setup · 01_bronze · 02_silver · 03_gold · 04_utils · 05_quality · 06_time_travel
sql/           ddl · analytics (15 scenarios) · time_travel
adf/           linkedServices · datasets · pipeline · trigger · arm_template
databricks/    workflows/job_ecommerce_medallion.json
docs/          HLD, LLD, code-flow, spec-improvements
```

---

## ✅ Prerequisites

- **Azure subscription** with permission to create: Data Factory, Databricks workspace,
  ADLS Gen2 storage account, Key Vault.
- **Databricks Runtime 13.3 LTS+** (Spark 3.5, Delta 3.x) with **Unity Catalog** enabled.
- The **Olist dataset** (9 CSVs) — download link is in the project spec PDF
  (Kaggle: *Brazilian E-Commerce Public Dataset by Olist*).
- For local testing only: Python 3.10+, Java 11/17, `pip install -r requirements.txt`.

---

## 🚀 Setup

### 1. Get the code into Databricks
```bash
# In the Databricks workspace: Repos → Add Repo → paste this git URL.
# Resulting workspace path (used by ADF/Workflow JSON):
#   /Repos/ecommerce/ecommerce-analytics-platform-brazil
```

### 2. Provision Azure storage
Create an ADLS Gen2 account with four containers: `landing`, `bronze`, `silver`, `gold`.
(Bronze/Silver/Gold can also be Unity Catalog managed tables — the default in this repo.)

### 3. Configure the pipeline
Edit **`config/pipeline_config.json`**:
- `storage.storage_account` → your ADLS account name
- the four `*_path` `abfss://…` URIs
- `environment.catalog` if not using `ecommerce`
- `scheduling.daily_trigger_time` (default `07:00`)

### 4. Store secrets in Key Vault
Add secrets referenced by the ADF linked services:
- `adls-account-key`
- `source-blob-connection-string`

Grant the **ADF managed identity** `get`/`list` on the Key Vault, and **Storage Blob Data
Contributor** on the ADLS account.

### 5. Create the catalog & schemas
Run once (or let the pipeline's first task do it):
```
notebooks/00_setup/00_create_catalog_schemas.py
```

### 6. Land the data
Drop the 9 CSVs in the source blob container `source-drop/` (ADF copies them to
`landing/olist/`). For a manual first run you can upload them straight to `landing/olist/`.

### 7. Deploy ingestion (Azure Data Factory)
Either:
- **Git integration (recommended):** point ADF at this repo, set the root to `adf/`, then
  **Publish**. Pipelines/datasets/linked services/trigger appear in the factory.
- **ARM template:** deploy `adf/arm_template/ARMTemplateForFactory.json` with
  `ARMTemplateParametersForFactory.json` (fill in the placeholders first).

Then **start the trigger** `tr_daily_7am` (it ships in `Stopped` state).

### 8. (Alternative) Orchestrate with Databricks Workflows
```bash
databricks jobs create --json @databricks/workflows/job_ecommerce_medallion.json
```
This runs setup → bronze → silver → gold → data_quality on a daily 07:00 cron.

---

## ▶️ Running the pipeline

| Mode | How |
|------|-----|
| Automated daily | ADF trigger `tr_daily_7am` (or the Databricks job cron) at 07:00 |
| Full run on demand | Trigger ADF pipeline `pl_ecommerce_medallion_daily`, or run the Databricks job now |
| Single layer | Run `notebooks/00_setup/run_bronze_all` / `run_silver_all` / `run_gold_all` |
| Single table | Run any `notebooks/02_silver/silver_*` or `03_gold/gold_*` notebook directly |

After a run, validate:
```sql
-- in Databricks SQL
SELECT * FROM ecommerce.gold.dq_results ORDER BY run_ts DESC;        -- quality gate
SELECT * FROM ecommerce.gold.fact_orders LIMIT 20;                   -- fact
```
Then open `sql/analytics/business_scenarios.sql` and run the 15 scenario queries, and
`sql/time_travel/time_travel_queries.sql` for the time-travel demos.

---

## 🕰 Time travel quick examples
```sql
DESCRIBE HISTORY ecommerce.gold.fact_orders;
SELECT * FROM ecommerce.gold.fact_orders VERSION AS OF 2;
SELECT * FROM ecommerce.gold.fact_orders TIMESTAMP AS OF date_sub(current_date(), 1);
-- RESTORE TABLE ecommerce.gold.fact_orders TO VERSION AS OF 3;   -- bad-load recovery
```

---

## 🧪 Local development (optional)
The notebooks use the Databricks `spark`/`dbutils` globals, so they run as-is on a cluster.
For pure-Python checks locally:
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m py_compile $(find notebooks -name '*.py')   # syntax check
```

---

## 🧱 Data model (star schema)
```
                 dim_date
                    │
 dim_customer ── fact_orders ── dim_product
                 │    │   │
        dim_seller    │   dim_geolocation
        (SCD-2)       │
                  dim_category
```
`fact_orders` grain = one order line item. See [LLD](docs/02_lld.md) for full column detail.

---

## 🛠 Troubleshooting
| Symptom | Fix |
|---------|-----|
| `Unknown dataset '<x>'` in Bronze | name must match `config/pipeline_config.json` sources |
| Path/abfss auth errors | check Key Vault secrets + ADF/Databricks identity RBAC |
| DQ job fails | inspect `ecommerce.gold.dq_results` — a FATAL check failed by design |
| Time travel "version not found" | the version was `VACUUM`-ed beyond retention |
| Unity Catalog not available | use Hive metastore: replace `ecommerce.bronze.x` with `bronze.x` |

---

## 📄 License / attribution
Built for the E-Commerce Analytics Platform (Brazil) spec by **Rahul M**, implemented by
**Malhar Jadhav**. Dataset © Olist (public, Kaggle).

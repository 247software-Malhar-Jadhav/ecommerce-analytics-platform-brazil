# 02 · Low-Level Design (LLD)

Table-by-table transformation logic, schemas, SCD mechanics, fact grain, surrogate keys,
partitioning and data-quality rules. Pairs with `docs/01_architecture_hld.md`.

---

## 1. Naming & Conventions

- Catalog: `ecommerce` · Schemas: `bronze`, `silver`, `gold`.
- Surrogate keys: `*_sk` = `xxhash64(business_key)` — deterministic & stable across runs.
- Audit columns: `ingest_ts` (Bronze), `updated_ts` (Silver/Gold), `source_file` (lineage).
- Time format for casts: `yyyy-MM-dd HH:mm:ss`.
- Money cleanup: strip everything except digits/`.`/`-`, cast to double (`clean_money`).
- Special-char cleanup: lowercase + collapse non-alphanumerics to `_` (`clean_special_chars`).

---

## 2. Bronze Layer (`notebooks/01_bronze/bronze_ingestion.py`)

Parametrised; one notebook ingests all nine sources. You pass **either** the short
`dataset` name (e.g. `customers`) **or** a raw `file_name` (e.g. `olist_customers_dataset.csv`)
— `resolve_source()` looks the other values up from `pipeline_config.json`, so dropping in a
file name lands it in the right Bronze table with the right schema automatically.

| Step | Action |
|------|--------|
| Read | `read_csv` with explicit schema, `PERMISSIVE` + `_rescued_data`, multiline/escape for review text |
| Enrich | add `ingest_ts = current_timestamp()` and `source_file` |
| Write | `append` to `ecommerce.bronze.<dataset>`, `mergeSchema=true` |
| Audit | `COMMENT ON TABLE`, `DESCRIBE HISTORY`, returns row-count JSON |

No casting/dedup/filter/join/rename — Bronze is faithful to source.

---

## 3. Silver Layer (`notebooks/02_silver/silver_*.py`)

Common pipeline: dedup latest-per-key on `ingest_ts` → drop null PKs → type/standardize →
`trim_strings` → drop `_rescued_data`/`source_file` → `add_audit_columns` → **overwrite**.

| Table | Key transformations | DQ |
|-------|--------------------|----|
| customers | zip → strip spaces → int; city → lower; state → UPPER | unique + non-null `customer_id` |
| orders | 5 timestamps → `to_timestamp`; status → lower | non-null `order_id`; est-delivery ≥ purchase (flag) |
| order_items | `order_item_id` → int; `shipping_limit_date` → ts; `price`/`freight_value` → `clean_money`; range ≥ 0 | invalid rows → `silver.order_items_errors` |
| payments | `payment_sequential`/`installments` → int; `payment_type` → `clean_special_chars` (credit@card→credit_card); `payment_value` → `clean_money` ≥ 0 | drop null/negative value |
| reviews | `review_score` → int, default 0, clamp 0–5; dates → ts | assert score ∈ [0,5] |
| products | fix Olist typo `lenght`→`length`; measurements → int, negatives → null; category → lower | — |
| sellers | zip → int; city → lower; state → UPPER | unique `seller_id` |
| geolocation | coords → double, filter to Brazil bounds; 1 representative row per zip (avg coords) | — |
| category_translation | normalize PT & EN names | unique `product_category_name` |

---

## 4. Gold Layer — Star Schema

### 4.1 Fact: `fact_orders` (`gold_fact_orders.py`)
- **Grain:** one row per `order_id` + `order_item_id`.
- **Joins:** order_items ⋈ orders ⋈ payments(order-level) ⋈ reviews(order-level) ⋈ dim_seller(current).
- **Payment allocation:** order `payment_value` allocated to each line by `price_share`
  (`item price ÷ order total price`) → revenue is **not** double-counted across items.
- **Reviews:** latest review per order (`deduplicate` on `review_creation_date`).
- **Measures:** `price`, `freight_value`, `payment_value`, `payment_installments`,
  `review_score` (default 0), `delivery_days` = `datediff(delivered_customer, purchase)`.
- **Keys:** `customer_sk`, `product_sk`, `seller_sk`, `date_id` (yyyyMMdd int) + business ids.
- **Load:** `left_anti` on natural key → append only new line items (idempotent re-runs).
- **Physical:** partitioned by `order_year_month`; `OPTIMIZE` after load; grain-uniqueness asserted.

### 4.2 Dimensions

| Dim | SCD | Mechanics |
|-----|-----|-----------|
| dim_customer | **SCD-1** | `MERGE` on `customer_id`; `whenMatchedUpdateAll` (overwrite) + `whenNotMatchedInsertAll`. Stable `customer_sk`. |
| dim_product  | **SCD-1** | same MERGE pattern on `product_id`; left-join English category. |
| dim_seller   | **SCD-2** | tracked attrs = `seller_city`, `seller_state`. See 4.3. |
| dim_category | **SCD-0** | static reference, full overwrite. |
| dim_date     | derived  | generated calendar 2016-01-01 → 2030-12-31, `date_id` = yyyyMMdd. |
| dim_geolocation | SCD-1 (optional) | one row per zip prefix. |

### 4.3 SCD-2 algorithm (`gold_dim_seller_scd2.py`)
Columns: `effective_from`, `effective_to` (high-date `9999-12-31`), `is_current`, version-unique `seller_sk`.

1. **First load:** write all sellers as current rows.
2. **Incremental:**
   - Compute `change_cond = (t.seller_city <> s.seller_city) OR (t.seller_state <> s.seller_state)`.
   - `joined` = source rows that are new (`t.seller_id IS NULL`) **or** changed.
   - **Expire:** `MERGE … WHEN MATCHED AND change_cond THEN UPDATE SET is_current=false, effective_to=now`.
   - **Insert:** append `joined` as new current versions.
3. **Invariant asserted:** exactly one `is_current = true` row per `seller_id`.

This satisfies the spec's SCD-2 steps: add effective_from / effective_to / is_current,
detect changes, expire old, insert new, maintain full history.

---

## 5. Surrogate Keys & Joins

| Fact column | Resolves to | Expression |
|-------------|-------------|------------|
| customer_sk | dim_customer | `xxhash64(customer_id)` |
| product_sk  | dim_product  | `xxhash64(product_id)` |
| seller_sk   | dim_seller (current at build time) | from `dim_seller` join |
| date_id     | dim_date     | `date_format(purchase_ts,'yyyyMMdd')` |

> Note: `customer_sk`/`product_sk` are computed identically in the fact and in the SCD-1
> dims (same `xxhash64`), so BI tools can join on either the surrogate or the business key.

---

## 6. Data Quality (`notebooks/05_quality/data_quality_checks.py`)

| # | Check | Severity |
|---|-------|----------|
| 1 | Row count > 0 for every Gold table | FATAL |
| 2 | No null PK / surrogate key | FATAL |
| 3 | No duplicate business/grain keys; one current SCD-2 row per seller | FATAL |
| 4 | Required fact columns present (schema validation) | FATAL |
| 5 | No orphan `customer_id` in fact (referential integrity) | WARN |

Results appended to `ecommerce.gold.dq_results`; job raises on any FATAL failure.

---

## 7. Time Travel (`06_time_travel/`, `sql/time_travel/`)

- `DESCRIBE HISTORY <table>` — version log.
- `VERSION AS OF n` / `TIMESTAMP AS OF ts` — point-in-time reads (PySpark helper
  `read_delta_version`).
- Version diff to count rows added by the last load.
- `RESTORE TABLE … TO VERSION AS OF n` — bad-load recovery.
- `VACUUM … RETAIN 168 HOURS` — retention window control.

---

## 8. Utility Functions (`notebooks/04_utils/common_functions.py`)

`load_config`, `read_csv`, `read_delta`, `read_delta_version`, `write_delta`,
`deduplicate`, `drop_null_keys`, `fill_defaults`, `trim_strings`, `validate_schema`,
`add_audit_columns`, `add_ingest_ts`, `table_exists`, `clean_money`, `clean_special_chars`.
Covers all five required utility functions (read / write Delta / dedup / null-handling /
schema validation) plus extras.

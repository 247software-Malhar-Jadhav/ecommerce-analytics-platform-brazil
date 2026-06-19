# 04 · Improvements & Clarifications to the Source Spec (PDF)

The implementation follows the spec faithfully. While building it, the following issues,
ambiguities and improvement opportunities were found and resolved. Each is listed with the
decision taken so reviewers can see *why* the code differs from a literal reading of the PDF.

---

## A. Corrections (spec was inconsistent or factually off)

1. **Schedule contradiction — 7 AM vs 7 PM.**
   *End Goal* says "Fully automated daily pipeline (7 AM)" but *Technology Stack* says
   "Scheduling – Daily at 7 PM".
   **Decision:** standardized on **07:00 America/São_Paulo** (the headline End-Goal value).
   Both the ADF trigger and the Databricks job cron use 07:00. Change in one place
   (`config/pipeline_config.json` + trigger) if 7 PM is intended.

2. **Raw column typo `lenght`.**
   The actual Olist files contain `product_name_lenght` and `product_description_lenght`
   (misspelled), while the PDF lists `product_name_length` / `product_description_length`.
   **Decision:** Bronze reads the raw misspelled names; Silver renames them to the correct
   `..._length` spelling (`silver_products.py`).

3. **`payment_value` aggregation vs fact grain — double-counting risk.**
   The fact grain is order *line item*, but `payment_value` is recorded at *order* level.
   Joining order-level payments to item-level rows multiplies revenue.
   **Decision:** allocate the order payment to each line by **price share**
   (`item_price / order_total_price`) in `gold_fact_orders.py`, so `SUM(payment_value)`
   equals true order revenue. Documented in LLD §4.1.

---

## B. Clarifications (spec under-specified; a decision was required)

4. **"Append mode" Bronze + daily full files ⇒ accumulating duplicates.**
   With append-only Bronze and full daily extracts, the same key appears many times.
   **Decision:** Silver deduplicates to the **latest row per business key** ordered by
   `ingest_ts` (`deduplicate(..., order_col="ingest_ts")`). This is what makes "incremental
   processing / no full reload downstream" actually work.

5. **`customer_zip_code_prefix` / `seller_zip_code_prefix` typed as INT.**
   Brazilian ZIP prefixes can have **leading zeros** (e.g. `01010`); casting to INT drops
   them. **Decision:** followed the spec (cast to INT) but flagging the trade-off — if ZIP is
   ever displayed or matched as text, store it as a zero-padded STRING instead.

6. **`payment_sequential` "Ignore".**
   It is needed as part of the natural key to deduplicate payments before aggregation.
   **Decision:** keep it through Silver for dedup, then aggregate it away at the order grain
   in the fact (it is not carried into `fact_orders`).

7. **SCD-2 change-detection timestamp.**
   The sellers source has no "last updated" column, so true change time is unknown.
   **Decision:** `effective_from` = load timestamp; change is detected by comparing tracked
   attributes (`seller_city`, `seller_state`) against the current dimension version.

8. **`dim_geolocation` has many rows per ZIP prefix.**
   Raw geolocation has multiple lat/lng points per prefix → not dimension-grain.
   **Decision:** collapse to **one representative row per ZIP** (mean lat/lng, modal
   city/state) in `silver_geolocation.py`.

9. **`review_score` default 0.**
   Scores are 1–5; `0` is used to mean "no/invalid review". To avoid skewing satisfaction
   metrics, all rating analytics filter `review_score > 0` (see `business_scenarios.sql` #8).

10. **`dim_date` not in the source list but `date_id` is a fact FK.**
    **Decision:** generate it (`gold_dim_date.py`) covering 2016–2030.

---

## C. Enhancements added beyond the spec

11. **Idempotent fact load.** `left_anti` on the natural key means re-running a day does not
    duplicate rows — safer than a blind append.
12. **Quarantine / error tables.** Invalid `order_items` rows are routed to
    `silver.order_items_errors` instead of being silently dropped (spec step 17 "optional").
13. **Data-quality gate.** `data_quality_checks.py` writes results to `gold.dq_results` and
    **fails the job** on FATAL violations, so bad data never reaches BI.
14. **Stable surrogate keys via `xxhash64`** — deterministic across runs, no identity-column
    coordination needed.
15. **Schema-on-read with `_rescued_data`** — Bronze never loses a malformed row.
16. **Secrets via Azure Key Vault** — no credentials in code or config.
17. **Two orchestrators provided** — ADF (spec's "ingestion with ADF") *and* a Databricks
    Workflows job (spec's stated orchestration tool).
18. **`OPTIMIZE` + partitioning** on the fact for query performance.

---

## D. Optional future improvements (not implemented)

- **Auto Loader / `cloudFiles`** for true streaming-style incremental file detection instead
  of ADF full copy.
- **Liquid Clustering** instead of static `order_year_month` partitioning on the fact.
- **Expectations framework** (e.g. DLT expectations or Great Expectations) for richer DQ.
- **Zero-padded STRING ZIP** + a geocoding enrichment step.
- **CDC from source systems** to drive genuinely incremental Bronze instead of daily full CSVs.

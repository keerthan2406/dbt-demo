# DBT-Databricks

A proof-of-concept medallion pipeline on Databricks (Unity Catalog):

1. **Bronze** – a Databricks Asset Bundle job loads daily landing CSVs into raw tables, driven by a control table.
2. **Silver / Gold** – a dbt project (`dbt-databricks`) cleans and tests the bronze data.

```
landing CSVs (UC volume)
        │  databricks/  (bundle job: 01_load_raw.py, COPY INTO)
        ▼
  <catalog>.bronze.*          all-STRING raw tables + audit columns
        │  dbt_proj_1/  (sources → staging views → silver/gold tables)
        ▼
  <catalog>.silver / staging / gold
```

## Repository layout

| Path | Purpose |
|---|---|
| `databricks/` | Databricks Asset Bundle (`dbt_poc_load_bronze`) for the bronze load |
| `databricks/src/00_create_control_file_extract.py` | Creates the control schemas, the `landing` volume, `control_file_extract` and `control_load_history`; upserts one config row per entity |
| `databricks/src/01_load_raw.py` | Metadata-driven loader: reads active rows of `control_file_extract`, loads each day's CSV into bronze |
| `databricks/resources/load_bronze_job.yml` | Job definition (`load_date` parameter, blank = today) |
| `databricks/landing_files/` | Sample daily extracts (`YYYY/MM/DD/<entity>_YYYY_MM_DD.csv`); not synced by the bundle |
| `dbt_proj_1/` | dbt project |
| `main.py`, `pyproject.toml` | Python project (uv); pins `dbt-core` and `dbt-databricks` |

## Bronze load

Entities are configured in `00_create_control_file_extract.py` (`ENTITIES`):

| Entity | Bronze table | Load type | Order |
|---|---|---|---|
| stores | `stores` | TRUNCATE | 1 |
| products | `products` | SCD1 | 2 |
| customers | `customers` | SCD2 | 3 |
| orders | `orders` | APPEND | 4 |
| order_items | `order_items` | APPEND | 5 |

For each active entity the loader:

- resolves `{YYYY}`, `{MM}`, `{DD}` and `{catalog}` in the volume path and file name from `load_date` and `catalog`;
- logs `FILE_NOT_FOUND` to `control_load_history` and skips the entity if the file is missing;
- creates the bronze table (all columns `STRING`) or truncates it, then runs `COPY INTO`;
- adds audit columns `_source_file`, `_file_date` and `_ingested_at`;
- fails the job at the end if any entity failed or was missing.

The load type is recorded in the control table. The loader always truncates and reloads, and SCD1, SCD2 and append history is handled downstream in dbt.

### Prerequisites

1. Run `00_create_control_file_extract` once per catalog.
2. Upload the CSVs to `/Volumes/<catalog>/landing/files/YYYY/MM/DD/`.

### Deploy and run

Run these from `databricks/`:

```bash
databricks bundle deploy -t dev
databricks bundle run load_bronze_job -t dev --params load_date=2026-11-10
```

| Target | Catalog | Notes |
|---|---|---|
| `dev` (default) | `dbt_project1_dev` | development mode |
| `prod` | `dbt_project1_prod` | scheduled daily at 06:00 Asia/Kolkata, runs as a service principal, emails on failure. Replace the `<PROD-SERVICE-PRINCIPAL-APPLICATION-ID>` placeholder in `databricks.yml` before deploying. |

## dbt project (`dbt_proj_1`)

Setup, from the repo root:

```bash
uv sync
cd dbt_proj_1
dbt deps        # installs dbt_utils
dbt seed
dbt build --vars '{load_date: 2026-11-10}'
```

Use `dbt build`, not `dbt run` followed by `dbt test`. `build` runs models, snapshots and tests in DAG order, so a failed test skips every downstream model.

The only var is `load_date`. It is required by the silver incremental `replace_where` predicates, the gate and the control-table logging. The other values are derived:

- **as_of_date:** the incremental silver models (`silver_orders`, `silver_order_items`) filter on `max(_file_date)` from the staging model, i.e. the file just loaded.
- **batch_id:** read from `control.control_load_history` (latest BRONZE batch for `load_date`), not passed in.

This needs a `dbt_proj_1` profile in your `profiles.yml` for the Databricks SQL warehouse. The bronze source reads from `{{ target.catalog }}.bronze`, so the profile must define `catalog`.

| Layer | Location | Materialization | Schema |
|---|---|---|---|
| Sources | `models/bronze/bronze.yml` | – | `bronze` |
| Staging | `models/silver/staging/` | view | `staging` |
| Silver | `models/silver/` | table (orders, order_items, products are incremental) | `silver` |
| Snapshot | `snapshots/customer_scd2.yml` | snapshot (timestamp on `_file_date`, hard deletes as new records) | `silver` |
| Gold | `models/gold/` | table | `gold` |
| Seeds | `seeds/` | table | `seeds` |

### Models

- **Staging** (`stg_customers`, `stg_orders`, `stg_order_items`, `stg_products`, `stg_stores`): trim and standardise case, cast types, dedupe, map country to region and currency, and map order status through the seed (`clean_status`, `is_revenue`).
- **Silver**: `silver_stores` (table), `silver_products` (incremental merge on `product_id`, SCD1), `silver_orders` and `silver_order_items` (incremental `replace_where` on `_file_date`). `customers_scd2` is the SCD2 snapshot of `stg_customers`.
- **Gold dimensions**: `dim_customers` (SCD2, one row per customer version with `valid_from`, `valid_to`, `is_current`, `is_deleted`), `dim_products` (SCD1), `dim_stores`. Surrogate keys use `dbt_utils.generate_surrogate_key`.
- **Gold facts and marts**:
  - `fct_orders`: one row per order; measures rolled up from order items; `customer_key` is a point-in-time lookup.
  - `fct_order_items`: one row per order line, with gross, discount, net, cost, profit and `margin_pct`.
  - `daily_sales`: by `order_date` and store region; revenue orders only (`is_revenue`).
  - `obt_sales`: `fct_order_items` joined to customer, product and store attributes.
- **Seeds**: `country_region_map`, `order_status_map`.

`macros/generate_schema_name.sql` overrides schema naming.

First run after the order model changes: run `silver_orders` once with `--full-refresh` so the incremental table picks up the new `order_ts` and `is_revenue` columns.

### Run gating and audit logging

No model or test hooks are used. Two project-level hooks in `dbt_project.yml` handle it:

- **`on-run-start`: `gate_bronze()`** (`macros/gate_bronze.sql`)
  - Fails the run if there is no BRONZE batch for `load_date`, or if the latest batch has any non-SUCCESS entity.
  - Stops the run if every BRONZE entity in the batch already has a GOLD SUCCESS logged after the bronze load finished.
- **`on-run-end`: `log_run_results(results)`** (`macros/log_run_results.sql`) writes one row per model, seed and test to `control.control_load_history`.

| Layer value | Seq | Rows |
|---|---|---|
| `STAGING` | 4 | models under `staging` |
| `SILVER` | 5 | other silver models |
| `GOLD` | 6 | gold models |
| `TEST` | 7 | data tests |

Status is `SUCCESS`, `FAILED`, `WARN` or `SKIPPED`. For tests, `rows_read` holds the failure count. Entity names drop the `stg_`, `silver_`, `dim_` and `fct_` prefixes so they match the bronze entity names.

dbt does not run hooks on tests, which is why test results are logged from `on-run-end` and not per test.

### Data tests

Staging tests, in `models/silver/staging/properties.yml`:

- `stg_customers`: `customer_id` not null and unique. `email` is non-empty and not `N/A` (`dbt_utils.expression_is_true`). `country_code` must exist in `country_region_map`.
- `stg_products`: `product_id` not null and unique. `category` not null (warn only). `unit_cost` and `unit_price` pass the custom generic test `is_neg_number`.
- `stg_stores`: `store_id` not null and unique.

Gold tests, in `models/gold/properties.yml`:

- Dimensions: surrogate keys and business ids are unique and not null. `dim_customers` also checks `valid_from <= valid_to` and uniqueness of `customer_id` plus `valid_from`.
- Facts: `order_id` and `order_item_id` are unique and not null. Customer, store and product keys have `relationships` tests (warn only), because orphan records such as customer `C999` are kept. `daily_sales` is unique on `order_date` plus `region`.

`is_neg_number` (`tests/generic/is_neg_number.sql`) fails on rows where the column is negative. NULLs are ignored.

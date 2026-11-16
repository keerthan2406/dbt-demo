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
dbt run
dbt test
```

This needs a `dbt_proj_1` profile in your `profiles.yml` for the Databricks SQL warehouse. The bronze source reads from `{{ target.catalog }}.bronze`, so the profile must define `catalog`.

| Layer | Location | Materialization | Schema |
|---|---|---|---|
| Sources | `models/bronze/bronze.yml` | – | `bronze` |
| Staging | `models/silver/staging/` | view | `staging` |
| Silver | `models/silver/` | table | `silver` |
| Gold | `models/gold/` | table | `gold` |
| Seeds | `seeds/` | table | `seeds` |

The silver and gold layers are configured but have no models yet. The staging models are `stg_customers`, `stg_orders`, `stg_order_items`, `stg_products` and `stg_stores`. They trim and standardise case, cast types and dedupe on the business key by keeping the latest `_ingested_at`. Seeds are `country_region_map` and `order_status_map`.

`macros/generate_schema_name.sql` overrides schema naming.

### Data tests

Defined in `models/silver/staging/properties.yml`:

- `stg_customers`: `customer_id` not null and unique. `email` is non-empty and not `N/A` (`dbt_utils.expression_is_true`). `country_code` must exist in `country_region_map`.
- `stg_products`: `product_id` not null and unique. `category` not null (warn only). `unit_cost` and `unit_price` pass the custom generic test `is_neg_number`.
- `stg_stores`: `store_id` not null and unique.

`is_neg_number` (`tests/generic/is_neg_number.sql`) fails on rows where the column is negative. NULLs are ignored.

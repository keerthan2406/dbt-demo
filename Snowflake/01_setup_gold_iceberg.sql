-- =====================================================================
-- Snowflake setup: read the Databricks gold layer as Iceberg tables
--
-- Values come from environment variables (see .env.example), so no host,
-- catalog or secret is stored in this file. Run it with the Snowflake CLI,
-- which renders the template variables from your environment:
--   set -a; source .env; set +a
--   snow sql -f Snowflake/01_setup_gold_iceberg.sql
--
-- Environment variables used:
--   DBT_DATABRICKS_HOST               Databricks host, no https:// (existing dbt variable)
--   DBT_DATABRICKS_CATALOG            Unity Catalog catalog name (existing dbt variable)
--   UC_SERVICE_PRINCIPAL_CLIENT_ID    service principal OAuth client id
--   UC_SERVICE_PRINCIPAL_SECRET       service principal OAuth secret
--
-- Prerequisites (Databricks side):
--   1. dbt build has run, so gold tables publish Iceberg metadata (UniForm).
--   2. External data access is enabled on the Unity Catalog metastore.
--   3. The service principal has EXTERNAL USE SCHEMA and SELECT on the
--      gold schema of that catalog.
--
-- Syntax for REST catalog integrations changes between Snowflake releases;
-- check the current docs if a clause is rejected.
-- =====================================================================

USE ROLE ACCOUNTADMIN;

-- 1) Demo objects ------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS demo_wh
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS demo_gold;
CREATE SCHEMA   IF NOT EXISTS demo_gold.gold;

USE WAREHOUSE demo_wh;
USE SCHEMA demo_gold.gold;

-- 2) Catalog integration to Databricks Unity Catalog -------------------
CREATE OR REPLACE CATALOG INTEGRATION uc_gold_int
  CATALOG_SOURCE    = ICEBERG_REST
  TABLE_FORMAT      = ICEBERG
  CATALOG_NAMESPACE = 'gold'
  REST_CONFIG = (
    CATALOG_URI            = 'https://<% ctx.env.DBT_DATABRICKS_HOST %>/api/2.1/unity-catalog/iceberg-rest'
    WAREHOUSE              = '<% ctx.env.DBT_DATABRICKS_CATALOG %>'
    ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS
  )
  REST_AUTHENTICATION = (
    TYPE                 = OAUTH
    OAUTH_TOKEN_URI      = 'https://<% ctx.env.DBT_DATABRICKS_HOST %>/oidc/v1/token'
    OAUTH_CLIENT_ID      = '<% ctx.env.UC_SERVICE_PRINCIPAL_CLIENT_ID %>'
    OAUTH_CLIENT_SECRET  = '<% ctx.env.UC_SERVICE_PRINCIPAL_SECRET %>'
    OAUTH_ALLOWED_SCOPES = ('all-apis')
  )
  ENABLED = TRUE;

-- Alternative auth: use a personal access token (variable UC_PAT) instead of OAuth by
-- replacing the REST_AUTHENTICATION block above with a BEARER type block.

-- 3) Iceberg tables over the gold layer --------------------------------
CREATE OR REPLACE ICEBERG TABLE dim_customers
  CATALOG = 'uc_gold_int' CATALOG_TABLE_NAME = 'dim_customers';

CREATE OR REPLACE ICEBERG TABLE dim_products
  CATALOG = 'uc_gold_int' CATALOG_TABLE_NAME = 'dim_products';

CREATE OR REPLACE ICEBERG TABLE dim_stores
  CATALOG = 'uc_gold_int' CATALOG_TABLE_NAME = 'dim_stores';

CREATE OR REPLACE ICEBERG TABLE fct_orders
  CATALOG = 'uc_gold_int' CATALOG_TABLE_NAME = 'fct_orders';

CREATE OR REPLACE ICEBERG TABLE fct_order_items
  CATALOG = 'uc_gold_int' CATALOG_TABLE_NAME = 'fct_order_items';

CREATE OR REPLACE ICEBERG TABLE daily_sales
  CATALOG = 'uc_gold_int' CATALOG_TABLE_NAME = 'daily_sales';

CREATE OR REPLACE ICEBERG TABLE obt_sales
  CATALOG = 'uc_gold_int' CATALOG_TABLE_NAME = 'obt_sales';

-- 4) Verify: compare with Databricks (SELECT COUNT(*) FROM <uc-catalog>.gold.<table>)
SELECT 'dim_customers'  AS table_name, COUNT(*) AS row_count FROM dim_customers
UNION ALL SELECT 'dim_products',   COUNT(*) FROM dim_products
UNION ALL SELECT 'dim_stores',     COUNT(*) FROM dim_stores
UNION ALL SELECT 'fct_orders',     COUNT(*) FROM fct_orders
UNION ALL SELECT 'fct_order_items',COUNT(*) FROM fct_order_items
UNION ALL SELECT 'daily_sales',    COUNT(*) FROM daily_sales
UNION ALL SELECT 'obt_sales',      COUNT(*) FROM obt_sales;

-- 5) After each dbt build, pick up new data if it does not appear:
--    ALTER ICEBERG TABLE fct_orders REFRESH;

-- =====================================================================
-- Snowflake setup: read the Databricks gold layer as Iceberg tables
-- Run in Cortex Code (CoCo), Snowsight, or any Snowflake SQL client.
--
-- Prerequisites (Databricks side):
--   1. dbt build has run, so gold tables publish Iceberg metadata (UniForm).
--   2. External data access is enabled on the Unity Catalog metastore.
--   3. A service principal has EXTERNAL USE SCHEMA and SELECT on
--      <uc_catalog>.gold, with an OAuth client id and secret.
--
-- Replace every <placeholder>. Never commit real secrets to Git.
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
--    <workspace-host>  e.g. dbc-xxxxxxxx-xxxx.cloud.databricks.com
--    <uc-catalog>      the Unity Catalog catalog name (dbt target catalog)
CREATE OR REPLACE CATALOG INTEGRATION uc_gold_int
  CATALOG_SOURCE    = ICEBERG_REST
  TABLE_FORMAT      = ICEBERG
  CATALOG_NAMESPACE = 'gold'
  REST_CONFIG = (
    CATALOG_URI            = 'https://<workspace-host>/api/2.1/unity-catalog/iceberg-rest'
    WAREHOUSE              = '<uc-catalog>'
    ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS
  )
  REST_AUTHENTICATION = (
    TYPE                 = OAUTH
    OAUTH_TOKEN_URI      = 'https://<workspace-host>/oidc/v1/token'
    OAUTH_CLIENT_ID      = '<service-principal-client-id>'
    OAUTH_CLIENT_SECRET  = '<service-principal-secret>'
    OAUTH_ALLOWED_SCOPES = ('all-apis')
  )
  ENABLED = TRUE;

-- Alternative auth (personal access token), replace REST_AUTHENTICATION above with:
--   REST_AUTHENTICATION = (TYPE = BEARER BEARER_TOKEN = '<databricks-pat>')

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

# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Create control tables & populate `control_file_extract` (re-runnable)
# MAGIC Set the widgets, run the cell. Safe to re-run: the table is created only if missing and rows are upserted by `entity_name`.
# MAGIC Add your own entity by appending one tuple to `ENTITIES`.

# COMMAND ----------
dbutils.widgets.text("catalog",         "dbt_project1_dev", "Catalog")
dbutils.widgets.text("control_schema",  "control",          "Control schema")
dbutils.widgets.text("landing_schema",  "landing",          "Landing schema (holds the volume)")
dbutils.widgets.text("volume",          "files",            "Volume name")

catalog, ctrl_schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("control_schema")
landing_schema, volume = dbutils.widgets.get("landing_schema"), dbutils.widgets.get("volume")
TBL = f"{catalog}.{ctrl_schema}.control_file_extract"

# 1) schemas + volume ---------------------------------------------------------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{ctrl_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{landing_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{landing_schema}.{volume}")

# 2) control table --------------------------------------------------------------
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TBL} (
  extract_id            BIGINT GENERATED ALWAYS AS IDENTITY,
  entity_name           STRING  NOT NULL COMMENT 'logical entity, unique',
  source_system         STRING  COMMENT 'where the file comes from',
  volume_base_path      STRING  COMMENT 'tokens: catalog placeholder, replaced at runtime',
  source_folder_format  STRING  COMMENT 'tokens: YYYY MM DD in braces - replaced from load_date at runtime',
  file_name_format      STRING  COMMENT 'tokens: YYYY MM DD in braces - e.g. order_YYYY_MM_DD.csv',
  target_table          STRING  COMMENT 'raw table name only (schema is fixed to raw)',
  column_list           STRING  COMMENT 'ordered source columns, comma separated',
  load_type             STRING  COMMENT 'TRUNCATE | SCD1 | SCD2 | APPEND',
  load_order            INT,
  is_active             BOOLEAN,
  created_at            TIMESTAMP,
  updated_at            TIMESTAMP
) USING DELTA""")

# 2b) load history table (written by the load notebook and the dbt hook)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{ctrl_schema}.control_load_history (
  load_history_id   BIGINT GENERATED ALWAYS AS IDENTITY,
  batch_id          STRING,
  dbt_invocation_id STRING,
  load_date         DATE,
  layer             STRING COMMENT 'LANDING | RAW | SEED | STAGING | SNAPSHOT | GOLD | DQ_TEST',
  layer_seq         INT,
  entity_name       STRING,
  object_name       STRING COMMENT 'file name, raw table or dbt node',
  source_path       STRING,
  target_table      STRING,
  load_type         STRING,
  status            STRING COMMENT 'SUCCESS | FAILED | SKIPPED | WARN | FILE_NOT_FOUND',
  rows_read         BIGINT,
  rows_written      BIGINT,
  rows_deleted      BIGINT,
  error_message     STRING,
  started_at        TIMESTAMP,
  ended_at          TIMESTAMP,
  duration_sec      DOUBLE,
  run_by            STRING,
  run_source        STRING
) USING DELTA""")

# 3) config rows ----------------------------------------------------------------
# {catalog} stays a literal token; the loader replaces it with the catalog widget at runtime.
BASE   = "/Volumes/{catalog}/" + landing_schema + "/" + volume
FOLDER = "{YYYY}/{MM}/{DD}"

#            entity,        file prefix,  target_table,  column_list,                                                                                      load_type, order
ENTITIES = [
 ("stores",      "store",      "stores",      "store_id,store_name,city,country_code,open_date,manager_name",                                   "TRUNCATE", 1),
 ("products",    "product",    "products",    "product_id,product_name,category,unit_price,unit_cost,updated_at",                               "SCD1",     2),
 ("customers",   "customer",   "customers",   "customer_id,first_name,last_name,email,phone,city,country_code,segment,signup_date,updated_at", "SCD2",     3),
 ("orders",      "order",      "orders",      "order_id,customer_id,store_id,order_ts,status,discount_pct,payment_method,ship_date",           "APPEND",   4),
 ("order_items", "order_item", "order_items", "order_item_id,order_id,product_id,quantity,unit_price",                                         "APPEND",   5),
]
rows = [(e, "LOCAL_UPLOAD", BASE, FOLDER, p + "_{YYYY}_{MM}_{DD}.csv", t, c, lt, o, True)
        for e, p, t, c, lt, o in ENTITIES]
cols = "entity_name, source_system, volume_base_path, source_folder_format, file_name_format, target_table, column_list, load_type, load_order, is_active"
spark.createDataFrame(rows, cols).createOrReplaceTempView("_cfe_src")

spark.sql(f"""
MERGE INTO {TBL} t
USING _cfe_src s
ON t.entity_name = s.entity_name
WHEN MATCHED THEN UPDATE SET
  t.source_system = s.source_system, t.volume_base_path = s.volume_base_path,
  t.source_folder_format = s.source_folder_format, t.file_name_format = s.file_name_format,
  t.target_table = s.target_table, t.column_list = s.column_list, t.load_type = s.load_type,
  t.load_order = s.load_order, t.is_active = s.is_active, t.updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT
  (entity_name, source_system, volume_base_path, source_folder_format, file_name_format,
   target_table, column_list, load_type, load_order, is_active, created_at, updated_at)
VALUES
  (s.entity_name, s.source_system, s.volume_base_path, s.source_folder_format, s.file_name_format,
   s.target_table, s.column_list, s.load_type, s.load_order, s.is_active, current_timestamp(), current_timestamp())""")

# 4) check ----------------------------------------------------------------------
display(spark.sql(f"""SELECT extract_id, entity_name, volume_base_path, source_folder_format, file_name_format,
                             target_table, load_type, load_order, is_active
                      FROM {TBL} ORDER BY load_order"""))

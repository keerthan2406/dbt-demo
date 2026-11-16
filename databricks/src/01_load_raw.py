# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Load landing CSVs into RAW (metadata driven)
# MAGIC * Reads `control_file_extract` (active rows, by `load_order`)
# MAGIC * Replaces `{YYYY}` `{MM}` `{DD}` `{catalog}` using the `load_date` / `catalog` widgets
# MAGIC * Creates the `bronze` schema if missing
# MAGIC * Per table: not there -> CREATE it | already there -> TRUNCATE it, then load the day's CSV (`COPY INTO`, all columns STRING + audit columns)
# MAGIC * SCD1 / SCD2 / append history is handled downstream by dbt (snapshot + incremental models), not by bronze
# MAGIC * Logs every file/table to `control_load_history` (table must already exist)
# MAGIC * Environment-agnostic: nothing is hard-coded. A Databricks job / bundle passes `catalog` (dev or prod) and `load_date`
# MAGIC
# MAGIC Prereq: run `00_create_control_file_extract`, create `control_load_history`, and upload the CSVs to the volume.

# COMMAND ----------
import os, uuid, datetime as dt
from pyspark.sql.types import *

dbutils.widgets.text("load_date",      "",                 "Load date YYYY-MM-DD (blank = today)")
dbutils.widgets.text("catalog",        "",                 "Catalog (required, e.g. dbt_project1_dev / dbt_project1_prod)")
dbutils.widgets.text("control_schema", "control",          "Control schema")
dbutils.widgets.text("bronze_schema",  "bronze",           "Bronze schema")
dbutils.widgets.text("batch_id",       "",                 "Batch id (blank = auto)")

load_date  = dt.date.fromisoformat(dbutils.widgets.get("load_date")) if dbutils.widgets.get("load_date").strip() else dt.date.today()
catalog    = dbutils.widgets.get("catalog").strip()
if not catalog:
    raise ValueError("Widget 'catalog' is required (bundle passes it per target: dev / prod)")
CTRL       = f"{catalog}.{dbutils.widgets.get('control_schema')}"
BRONZE     = f"{catalog}.{dbutils.widgets.get('bronze_schema')}"
batch_id   = dbutils.widgets.get("batch_id") or str(uuid.uuid4())
run_by     = spark.sql("select current_user()").first()[0]

# fixed file settings (same for all entities)
FILE_FORMAT, DELIMITER, HEADER = "CSV", ",", "true"

def resolve(s: str) -> str:
    """order_{YYYY}_{MM}_{DD}.csv -> order_2026_11_10.csv ; /Volumes/{catalog}/... -> /Volumes/<catalog>/..."""
    return (s.replace("{YYYY}", f"{load_date:%Y}").replace("{MM}", f"{load_date:%m}")
             .replace("{DD}", f"{load_date:%d}").replace("{catalog}", catalog))

# file date is parsed from the FILE NAME (..._2026_11_10.csv) of every loaded file
FILE_DATE_EXPR = "to_date(regexp_extract(_metadata.file_path, '([0-9]{4}_[0-9]{2}_[0-9]{2})[.]csv$', 1), 'yyyy_MM_dd')"

# COMMAND ----------
# ---- bronze schema + history table check ----
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE}")
if not spark.catalog.tableExists(f"{CTRL}.control_load_history"):
    raise Exception(f"{CTRL}.control_load_history not found - create it first")

HIST_COLS = ["batch_id","dbt_invocation_id","load_date","layer","layer_seq","entity_name","object_name","source_path",
             "target_table","load_type","status","rows_read","rows_written","rows_deleted","error_message",
             "started_at","ended_at","duration_sec","run_by","run_source"]
HIST_SCHEMA = StructType([
    StructField("batch_id", StringType()), StructField("dbt_invocation_id", StringType()), StructField("load_date", DateType()),
    StructField("layer", StringType()), StructField("layer_seq", IntegerType()), StructField("entity_name", StringType()),
    StructField("object_name", StringType()), StructField("source_path", StringType()), StructField("target_table", StringType()),
    StructField("load_type", StringType()), StructField("status", StringType()), StructField("rows_read", LongType()),
    StructField("rows_written", LongType()), StructField("rows_deleted", LongType()), StructField("error_message", StringType()),
    StructField("started_at", TimestampType()), StructField("ended_at", TimestampType()), StructField("duration_sec", DoubleType()),
    StructField("run_by", StringType()), StructField("run_source", StringType())])

def log(layer, seq, entity, obj, status, started, source_path=None, target=None, load_type=None,
        rows_read=None, rows_written=None, rows_deleted=None, error=None):
    ended = dt.datetime.now()
    row = (batch_id, None, load_date, layer, seq, entity, obj, source_path, target, load_type, status,
           rows_read, rows_written, rows_deleted, str(error)[:4000] if error else None,
           started, ended, round((ended - started).total_seconds(), 2), run_by, "databricks_notebook")
    spark.createDataFrame([row], HIST_SCHEMA).createOrReplaceTempView("_hist_row")
    cols = ", ".join(HIST_COLS)
    spark.sql(f"INSERT INTO {CTRL}.control_load_history ({cols}) SELECT {cols} FROM _hist_row")

# COMMAND ----------
cfg = spark.table(f"{CTRL}.control_file_extract").where("is_active").orderBy("load_order").collect()
if not cfg:
    raise Exception(f"No active rows in {CTRL}.control_file_extract - run 00_create_control_file_extract first")

failures = []
print(f"catalog={catalog}  batch_id={batch_id}  load_date={load_date}  entities={[r.entity_name for r in cfg]}\n")

for r in cfg:
    entity = r.entity_name
    fname  = resolve(r.file_name_format)
    path   = f"{resolve(r.volume_base_path)}/{resolve(r.source_folder_format)}/{fname}"
    target = f"{BRONZE}.{r.target_table}"
    cols   = r.column_list

    # ---------- 1) LANDING: is the expected file there? ----------
    t0 = dt.datetime.now()
    if not os.path.exists(path):
        log("LANDING", 1, entity, fname, "FILE_NOT_FOUND", t0, source_path=path, error=f"expected file not found: {path}")
        failures.append(entity); print(f"[FILE_NOT_FOUND] {entity}: {path}")
        continue                                   # nothing is truncated/loaded for this entity
    src_rows = spark.read.option("header", HEADER).option("delimiter", DELIMITER).csv(path).count()
    log("LANDING", 1, entity, fname, "SUCCESS", t0, source_path=path, rows_read=src_rows)

    # ---------- 2) BRONZE: create table if missing, else truncate; then load ----------
    t1 = dt.datetime.now()
    try:
        deleted = None
        if spark.catalog.tableExists(target):
            deleted = spark.table(target).count()
            spark.sql(f"TRUNCATE TABLE {target}")
            action = "truncated"
        else:
            col_defs = ", ".join(f"{c.strip()} STRING" for c in cols.split(","))
            spark.sql(f"""CREATE TABLE {target}
                          ({col_defs}, _source_file STRING, _file_date DATE, _ingested_at TIMESTAMP)""")
            action = "created"

        res = spark.sql(f"""
          COPY INTO {target}
          FROM (SELECT {cols},
                       _metadata.file_path AS _source_file,
                       {FILE_DATE_EXPR}     AS _file_date,
                       current_timestamp()  AS _ingested_at
                FROM '{path}')
          FILEFORMAT = {FILE_FORMAT}
          FORMAT_OPTIONS ('header' = '{HEADER}', 'delimiter' = '{DELIMITER}', 'inferSchema' = 'false')
          COPY_OPTIONS ('force' = 'true')""").first()
        inserted = int((res.asDict() if res else {}).get("num_inserted_rows") or 0)
        log("RAW", 2, entity, r.target_table, "SUCCESS", t1, source_path=path, target=target,
            load_type=r.load_type, rows_read=src_rows, rows_written=inserted, rows_deleted=deleted)
        print(f"[SUCCESS] {entity:12s} table {action}; {inserted:>4} rows -> {target}")
    except Exception as e:
        log("RAW", 2, entity, r.target_table, "FAILED", t1, source_path=path, target=target,
            load_type=r.load_type, rows_read=src_rows, error=e)
        failures.append(entity); print(f"[FAILED] {entity}: {e}")

# COMMAND ----------
display(spark.sql(f"""
  SELECT layer, entity_name, object_name, load_type, status, rows_read, rows_written, rows_deleted, duration_sec, error_message
  FROM {CTRL}.control_load_history WHERE batch_id = '{batch_id}' ORDER BY load_history_id"""))

if failures:
    raise Exception(f"Batch {batch_id}: failed / missing file for {failures}")
dbutils.notebook.exit(batch_id)     # optional: pass to dbt with --vars '{batch_id: <value>}'

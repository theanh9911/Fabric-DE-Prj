# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "79063a99-fe42-4078-8659-25c7c2894c06",
# META       "default_lakehouse_name": "lh_platform",
# META       "default_lakehouse_workspace_id": "02594272-f3b4-47d6-a983-347bdaefe9be",
# META       "known_lakehouses": [
# META         {
# META           "id": "79063a99-fe42-4078-8659-25c7c2894c06"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_setup_ddl
# Tạo schema, bảng, view của `meta`, `slv`, `gld` (docs/04-kien-truc.md §4–§7); idempotent (`IF NOT EXISTS` /
# `OR REPLACE VIEW`). Các khối SQL phải khớp 04 (lint C11). Không bao giờ tự xoá dữ liệu.
#
# `p_reset = True` và `p_confirm = "RESET"`: xoá mọi object trong `meta`, `slv`, `gld` và `Files/landing`,
# `Files/_staging` rồi tạo lại. Chỉ dùng ở Dev (docs/07 §1.2).

# PARAMETERS CELL ********************

p_reset = False
p_confirm = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if p_reset:
    if p_confirm != "RESET":
        raise ValueError('p_reset = True cần p_confirm = "RESET"')
    for schema in ("meta", "slv", "gld"):
        if not spark.catalog.databaseExists(schema):
            continue
        for obj in spark.sql(f"SHOW TABLES IN {schema}").collect():
            name = f"{schema}.{obj['tableName']}"
            if spark.catalog.getTable(name).tableType.upper() == "VIEW":
                spark.sql(f"DROP VIEW IF EXISTS {name}")
        for obj in spark.sql(f"SHOW TABLES IN {schema}").collect():
            spark.sql(f"DROP TABLE IF EXISTS {schema}.{obj['tableName']}")
    for path in ("Files/landing", "Files/_staging"):
        if notebookutils.fs.exists(path):
            notebookutils.fs.rm(path, True)
    print("RESET xong: meta, slv, gld, Files/landing, Files/_staging")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS meta;
# MAGIC CREATE SCHEMA IF NOT EXISTS slv;
# MAGIC CREATE SCHEMA IF NOT EXISTS gld;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Meta: bảng trạng thái và log

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS meta.delivery_log (
# MAGIC   delivery_id STRING NOT NULL, status STRING NOT NULL,
# MAGIC   batch_id STRING NOT NULL, source_system STRING NOT NULL, entity STRING NOT NULL,
# MAGIC   contract_version STRING, load_strategy STRING, kind STRING, landing_path STRING,
# MAGIC   source_files ARRAY<STRING>,
# MAGIC   delivered_at_utc TIMESTAMP, snapshot_date DATE, window_start STRING, window_end STRING,
# MAGIC   rows_landed BIGINT, source_count BIGINT, source_checksum STRING,
# MAGIC   schema_added ARRAY<STRING>, schema_missing ARRAY<STRING>,
# MAGIC   rows_read BIGINT, rows_accepted BIGINT, rows_rejected BIGINT, rows_flagged BIGINT,
# MAGIC   logic_version INT, run_id STRING NOT NULL, event_at_utc TIMESTAMP NOT NULL, error_message STRING
# MAGIC ) USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS meta.landed_file (
# MAGIC   source_system STRING NOT NULL, entity STRING NOT NULL, file_name STRING NOT NULL,
# MAGIC   file_size BIGINT NOT NULL, modify_time_utc TIMESTAMP NOT NULL,
# MAGIC   row_count BIGINT, file_checksum STRING,
# MAGIC   outcome STRING NOT NULL,          -- LANDED | UNCHANGED | SUPERSEDED
# MAGIC   delivery_id STRING, batch_id STRING NOT NULL, run_id STRING NOT NULL, landed_at_utc TIMESTAMP NOT NULL
# MAGIC ) USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS meta.run_log (
# MAGIC   run_id STRING NOT NULL, step STRING NOT NULL, layer STRING NOT NULL,
# MAGIC   event STRING NOT NULL, reason STRING, exec_mode STRING,
# MAGIC   run_mode STRING, batch_from STRING, batch_to STRING,
# MAGIC   logic_version INT, input_versions MAP<STRING, BIGINT>, output_version BIGINT,
# MAGIC   rows_in BIGINT, rows_inserted BIGINT, rows_updated BIGINT, rows_deleted BIGINT,
# MAGIC   rows_rejected BIGINT, rows_flagged BIGINT,
# MAGIC   params STRING, event_at_utc TIMESTAMP NOT NULL, error_message STRING
# MAGIC ) USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS meta.check_result (
# MAGIC   run_id STRING NOT NULL, batch_id STRING, check_id STRING NOT NULL,
# MAGIC   source STRING NOT NULL,           -- platform | contract | recon
# MAGIC   check_type STRING, object_name STRING, scope_value STRING,
# MAGIC   expected DECIMAL(38,4), actual DECIMAL(38,4), threshold DECIMAL(38,4),
# MAGIC   severity STRING NOT NULL,         -- critical | warning
# MAGIC   status STRING NOT NULL,           -- PASS | WARN | FAIL
# MAGIC   message STRING, sample STRING, checked_at_utc TIMESTAMP NOT NULL
# MAGIC ) USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS meta.table_state (
# MAGIC   target_table STRING NOT NULL, logic_version INT NOT NULL,
# MAGIC   input_versions MAP<STRING, BIGINT>,   -- version đã xử lý tới (bao gồm) của từng input bảng
# MAGIC   output_version BIGINT NOT NULL, last_run_id STRING NOT NULL,
# MAGIC   last_exec_mode STRING NOT NULL, last_success_at_utc TIMESTAMP NOT NULL
# MAGIC ) USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Meta: view

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE VIEW meta.v_delivery_state AS
# MAGIC SELECT * FROM (
# MAGIC   SELECT d.*, row_number() OVER (PARTITION BY delivery_id ORDER BY event_at_utc DESC) AS _rn
# MAGIC   FROM meta.delivery_log d) x
# MAGIC WHERE _rn = 1;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE VIEW meta.v_pending_delivery AS
# MAGIC SELECT d.* FROM meta.v_delivery_state d
# MAGIC WHERE d.status = 'LANDED'
# MAGIC    OR (d.status = 'FAILED' AND NOT EXISTS (
# MAGIC         SELECT 1 FROM meta.v_delivery_state n
# MAGIC         WHERE n.status = 'LOADED' AND n.source_system = d.source_system AND n.entity = d.entity
# MAGIC           AND n.delivered_at_utc > d.delivered_at_utc
# MAGIC           AND (d.kind = 'snapshot' OR size(d.source_files) = 0
# MAGIC                OR arrays_overlap(n.source_files, d.source_files))));

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE VIEW meta.v_watermark AS
# MAGIC SELECT source_system, entity, max(window_end) AS watermark
# MAGIC FROM meta.v_delivery_state
# MAGIC WHERE status = 'LOADED' AND load_strategy = 'db_incremental'
# MAGIC GROUP BY source_system, entity;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE VIEW meta.v_last_snapshot_checksum AS
# MAGIC SELECT source_system, entity, max_by(source_checksum, delivered_at_utc) AS source_checksum
# MAGIC FROM meta.v_delivery_state
# MAGIC WHERE status = 'LOADED' AND snapshot_date IS NOT NULL
# MAGIC GROUP BY source_system, entity;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE VIEW meta.v_run_latest AS
# MAGIC SELECT r.* FROM meta.run_log r
# MAGIC JOIN (SELECT run_id FROM meta.run_log WHERE step = 'pipeline' AND event = 'STARTED'
# MAGIC       ORDER BY event_at_utc DESC LIMIT 1) l ON r.run_id = l.run_id;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Silver: bảng

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS slv.orders_event (
# MAGIC   source_system STRING NOT NULL, order_no STRING NOT NULL, product_code STRING NOT NULL,
# MAGIC   customer_code STRING, salesman_code STRING, order_date DATE,
# MAGIC   order_status STRING NOT NULL, order_status_raw STRING, status_rank INT NOT NULL,
# MAGIC   quantity DECIMAL(18,2), price DECIMAL(18,2),
# MAGIC   tax_rate DECIMAL(9,6), tax_rate_raw STRING,
# MAGIC   created_at_utc TIMESTAMP, updated_at_utc TIMESTAMP NOT NULL,
# MAGIC   _bk STRING NOT NULL, _event_key STRING NOT NULL, _record_hash STRING NOT NULL,
# MAGIC   _batch_id STRING NOT NULL, _delivery_id STRING NOT NULL, _delivered_at_utc TIMESTAMP NOT NULL,
# MAGIC   _source_file STRING, _row_no BIGINT, _dq_flags ARRAY<STRING>,
# MAGIC   _run_id STRING NOT NULL, _logic_version INT NOT NULL, _ingested_at_utc TIMESTAMP NOT NULL
# MAGIC ) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS slv.customers_history (
# MAGIC   source_system STRING NOT NULL, customer_code STRING NOT NULL,
# MAGIC   customer_name STRING, country STRING, city STRING, gender STRING, gender_raw STRING, address STRING,
# MAGIC   created_at_utc TIMESTAMP, updated_at_utc TIMESTAMP,
# MAGIC   is_deleted BOOLEAN NOT NULL, _snapshot_date DATE NOT NULL, _change_kind STRING NOT NULL,
# MAGIC   _bk STRING NOT NULL, _record_hash STRING NOT NULL,
# MAGIC   _batch_id STRING NOT NULL, _delivery_id STRING NOT NULL, _delivered_at_utc TIMESTAMP NOT NULL,
# MAGIC   _source_file STRING, _row_no BIGINT, _dq_flags ARRAY<STRING>,
# MAGIC   _run_id STRING NOT NULL, _logic_version INT NOT NULL, _ingested_at_utc TIMESTAMP NOT NULL
# MAGIC ) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS slv.products_history (
# MAGIC   source_system STRING NOT NULL, product_code STRING NOT NULL,
# MAGIC   product_name STRING, brand STRING, brand_raw STRING, category_code STRING,
# MAGIC   created_at_utc TIMESTAMP, updated_at_utc TIMESTAMP,
# MAGIC   is_deleted BOOLEAN NOT NULL, _snapshot_date DATE NOT NULL, _change_kind STRING NOT NULL,
# MAGIC   _bk STRING NOT NULL, _record_hash STRING NOT NULL,
# MAGIC   _batch_id STRING NOT NULL, _delivery_id STRING NOT NULL, _delivered_at_utc TIMESTAMP NOT NULL,
# MAGIC   _source_file STRING, _row_no BIGINT, _dq_flags ARRAY<STRING>,
# MAGIC   _run_id STRING NOT NULL, _logic_version INT NOT NULL, _ingested_at_utc TIMESTAMP NOT NULL
# MAGIC ) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS slv.sales_hierarchy_history (
# MAGIC   source_system STRING NOT NULL, salesman_code STRING NOT NULL,
# MAGIC   salesman_name STRING, division STRING, position STRING, position_raw STRING,
# MAGIC   salesmanager_code STRING, salesmanager_name STRING,
# MAGIC   created_at_utc TIMESTAMP, updated_at_utc TIMESTAMP,
# MAGIC   is_deleted BOOLEAN NOT NULL, _snapshot_date DATE NOT NULL, _change_kind STRING NOT NULL,
# MAGIC   _bk STRING NOT NULL, _record_hash STRING NOT NULL,
# MAGIC   _batch_id STRING NOT NULL, _delivery_id STRING NOT NULL, _delivered_at_utc TIMESTAMP NOT NULL,
# MAGIC   _source_file STRING, _row_no BIGINT, _dq_flags ARRAY<STRING>,
# MAGIC   _run_id STRING NOT NULL, _logic_version INT NOT NULL, _ingested_at_utc TIMESTAMP NOT NULL
# MAGIC ) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS slv.categories_history (
# MAGIC   source_system STRING NOT NULL, category_code STRING NOT NULL,
# MAGIC   category_lvl1 STRING, category_lvl2 STRING, category_lvl3 STRING, category_lvl4 STRING,
# MAGIC   is_deleted BOOLEAN NOT NULL, _snapshot_date DATE NOT NULL, _change_kind STRING NOT NULL,
# MAGIC   _bk STRING NOT NULL, _record_hash STRING NOT NULL,
# MAGIC   _batch_id STRING NOT NULL, _delivery_id STRING NOT NULL, _delivered_at_utc TIMESTAMP NOT NULL,
# MAGIC   _source_file STRING, _row_no BIGINT, _dq_flags ARRAY<STRING>,
# MAGIC   _run_id STRING NOT NULL, _logic_version INT NOT NULL, _ingested_at_utc TIMESTAMP NOT NULL
# MAGIC ) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS slv.quarantine (
# MAGIC   entity STRING NOT NULL, source_system STRING NOT NULL,
# MAGIC   batch_id STRING NOT NULL, delivery_id STRING NOT NULL, business_key STRING,
# MAGIC   reason STRING NOT NULL, action STRING NOT NULL,     -- REJECT | FLAG
# MAGIC   row_json STRING NOT NULL, run_id STRING NOT NULL, quarantined_at_utc TIMESTAMP NOT NULL,
# MAGIC   resolved_at_utc TIMESTAMP, resolved_run_id STRING
# MAGIC ) USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Silver: quarantine view

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE VIEW slv.v_orders_current AS
# MAGIC SELECT * FROM (
# MAGIC   SELECT e.*, row_number() OVER (
# MAGIC            PARTITION BY source_system, order_no, product_code
# MAGIC            ORDER BY updated_at_utc DESC, status_rank DESC, _delivered_at_utc DESC, _row_no DESC) AS _rn
# MAGIC   FROM slv.orders_event e) x
# MAGIC WHERE _rn = 1;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE VIEW slv.v_quarantine_open AS
# MAGIC SELECT * FROM slv.quarantine WHERE resolved_at_utc IS NULL;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Silver: view hiện hành

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE VIEW slv.v_customers_current AS
# MAGIC SELECT * FROM (
# MAGIC   SELECT h.*, row_number() OVER (PARTITION BY source_system, customer_code
# MAGIC                                  ORDER BY _snapshot_date DESC, _delivered_at_utc DESC) AS _rn
# MAGIC   FROM slv.customers_history h) x
# MAGIC WHERE _rn = 1;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE VIEW slv.v_products_current AS
# MAGIC SELECT * FROM (
# MAGIC   SELECT h.*, row_number() OVER (PARTITION BY source_system, product_code
# MAGIC                                  ORDER BY _snapshot_date DESC, _delivered_at_utc DESC) AS _rn
# MAGIC   FROM slv.products_history h) x
# MAGIC WHERE _rn = 1;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE VIEW slv.v_sales_hierarchy_current AS
# MAGIC SELECT * FROM (
# MAGIC   SELECT h.*, row_number() OVER (PARTITION BY source_system, salesman_code
# MAGIC                                  ORDER BY _snapshot_date DESC, _delivered_at_utc DESC) AS _rn
# MAGIC   FROM slv.sales_hierarchy_history h) x
# MAGIC WHERE _rn = 1;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE VIEW slv.v_categories_current AS
# MAGIC SELECT * FROM (
# MAGIC   SELECT h.*, row_number() OVER (PARTITION BY source_system, category_code
# MAGIC                                  ORDER BY _snapshot_date DESC, _delivered_at_utc DESC) AS _rn
# MAGIC   FROM slv.categories_history h) x
# MAGIC WHERE _rn = 1;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold: bảng

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS gld.dim_date (
# MAGIC   date_key INT NOT NULL, date_value DATE NOT NULL,
# MAGIC   year INT, quarter INT, month_key INT, month INT, month_name STRING,
# MAGIC   day INT, day_of_week INT, day_name STRING, iso_week INT, is_weekend BOOLEAN,
# MAGIC   _run_id STRING NOT NULL, _logic_version INT NOT NULL, _updated_at_utc TIMESTAMP NOT NULL
# MAGIC ) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS gld.dim_customer (
# MAGIC   customer_sk BIGINT NOT NULL, customer_code STRING NOT NULL,
# MAGIC   customer_name STRING, country STRING, city STRING, gender STRING, address STRING,
# MAGIC   source_systems STRING NOT NULL,
# MAGIC   is_inferred BOOLEAN NOT NULL, is_deleted BOOLEAN NOT NULL, _dim_hash STRING NOT NULL,
# MAGIC   _run_id STRING NOT NULL, _logic_version INT NOT NULL, _updated_at_utc TIMESTAMP NOT NULL
# MAGIC ) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS gld.dim_product (
# MAGIC   product_sk BIGINT NOT NULL, product_code STRING NOT NULL,
# MAGIC   product_name STRING, brand STRING, category_code STRING, category_path STRING,
# MAGIC   category_l1 STRING, category_l2 STRING, category_l3 STRING, category_l4 STRING,
# MAGIC   source_systems STRING NOT NULL,
# MAGIC   is_inferred BOOLEAN NOT NULL, is_deleted BOOLEAN NOT NULL, _dim_hash STRING NOT NULL,
# MAGIC   _run_id STRING NOT NULL, _logic_version INT NOT NULL, _updated_at_utc TIMESTAMP NOT NULL
# MAGIC ) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS gld.dim_salesman (
# MAGIC   salesman_sk BIGINT NOT NULL, source_system STRING NOT NULL, salesman_code STRING NOT NULL,
# MAGIC   salesman_name STRING, division STRING, position STRING,
# MAGIC   manager_code STRING, manager_name STRING,
# MAGIC   valid_from DATE NOT NULL, valid_to DATE NOT NULL, is_current BOOLEAN NOT NULL,
# MAGIC   is_deleted BOOLEAN NOT NULL, is_inferred BOOLEAN NOT NULL, _scd_hash STRING NOT NULL,
# MAGIC   _run_id STRING NOT NULL, _logic_version INT NOT NULL, _updated_at_utc TIMESTAMP NOT NULL
# MAGIC ) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS gld.fct_order_line (
# MAGIC   source_system STRING NOT NULL, order_no STRING NOT NULL, product_code STRING NOT NULL,
# MAGIC   customer_code STRING, salesman_code STRING,
# MAGIC   customer_sk BIGINT NOT NULL, product_sk BIGINT NOT NULL, salesman_sk BIGINT NOT NULL,
# MAGIC   order_date_key INT NOT NULL,
# MAGIC   current_status STRING NOT NULL,
# MAGIC   quantity DECIMAL(18,2) NOT NULL, price DECIMAL(18,2) NOT NULL, tax_rate DECIMAL(9,6),
# MAGIC   first_pending_at_utc TIMESTAMP, first_confirmed_at_utc TIMESTAMP, first_shipped_at_utc TIMESTAMP,
# MAGIC   first_delivered_at_utc TIMESTAMP, first_returned_at_utc TIMESTAMP, first_cancelled_at_utc TIMESTAMP,
# MAGIC   last_event_at_utc TIMESTAMP NOT NULL, status_event_count INT NOT NULL,
# MAGIC   _row_hash STRING NOT NULL,
# MAGIC   _run_id STRING NOT NULL, _logic_version INT NOT NULL, _updated_at_utc TIMESTAMP NOT NULL
# MAGIC ) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

expected = {
    "meta": {"delivery_log", "landed_file", "run_log", "check_result", "table_state", "v_delivery_state",
             "v_pending_delivery", "v_watermark", "v_last_snapshot_checksum", "v_run_latest"},
    "slv": {"orders_event", "customers_history", "products_history", "sales_hierarchy_history",
            "categories_history", "quarantine", "v_orders_current", "v_customers_current", "v_products_current",
            "v_sales_hierarchy_current", "v_categories_current", "v_quarantine_open"},
    "gld": {"dim_date", "dim_customer", "dim_product", "dim_salesman", "fct_order_line"},
}
missing = []
for schema, names in expected.items():
    have = {r["tableName"] for r in spark.sql(f"SHOW TABLES IN {schema}").collect()}
    missing += [f"{schema}.{n}" for n in sorted(names - have)]
cdf_off = []
for schema in ("slv", "gld"):
    for n in sorted(expected[schema]):
        if n.startswith("v_") or n == "quarantine":
            continue
        props = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {schema}.{n}").collect()}
        if props.get("delta.enableChangeDataFeed") != "true":
            cdf_off.append(f"{schema}.{n}")
if missing or cdf_off:
    raise RuntimeError(f"thiếu object: {missing}; CDF chưa bật: {cdf_off}")
print("nb_setup_ddl OK: đủ object, CDF bật")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

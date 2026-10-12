# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

p_clean_schema = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # nb_setup_ddl
# Idempotent bootstrap for the architecture in docs/ARCHITECTURE.md.
# Landing is the only raw layer; no Bronze schema or config tables are created.
# Set p_clean_schema=True only for a fresh Dev reset; it drops managed tables/views
# in slv, gld and meta but deliberately preserves Files/landing.

# CELL ********************

if p_clean_schema:
    for schema_name in ["slv", "gld", "meta"]:
        try:
            objects = spark.sql(f"SHOW TABLES IN {schema_name}").collect()
        except Exception:
            objects = []
        for obj in objects:
            object_name = f"{schema_name}.{obj['tableName']}"
            spark.sql(f"DROP VIEW IF EXISTS {object_name}")
            spark.sql(f"DROP TABLE IF EXISTS {object_name}")

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS slv;
# MAGIC CREATE SCHEMA IF NOT EXISTS gld;
# MAGIC CREATE SCHEMA IF NOT EXISTS meta;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Delivery state

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS meta.ingestion_batch (
# MAGIC   batch_id STRING, source_system STRING, entity STRING, delivery_id STRING,
# MAGIC   delivered_at_utc TIMESTAMP, load_strategy STRING, window_start STRING, window_end STRING,
# MAGIC   landing_path STRING, files_written INT, rows_copied BIGINT, source_checksum STRING,
# MAGIC   rows_read BIGINT, rows_changed BIGINT, rows_unchanged BIGINT, rows_quarantined BIGINT,
# MAGIC   watermark_value STRING, schema_changes STRING, status STRING,
# MAGIC   loaded_run_id STRING, loaded_at_utc TIMESTAMP, error_message STRING
# MAGIC ) USING DELTA;
# MAGIC ALTER TABLE meta.ingestion_batch SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS meta.table_run (
# MAGIC   run_id STRING, batch_from STRING, batch_to STRING, run_mode STRING, layer STRING,
# MAGIC   table_name STRING, event STRING, rows_in BIGINT, rows_inserted BIGINT,
# MAGIC   rows_updated BIGINT, rows_deleted BIGINT, rows_quarantined BIGINT, delta_version BIGINT,
# MAGIC   source_versions MAP<STRING, BIGINT>, event_at_utc TIMESTAMP, error_message STRING
# MAGIC ) USING DELTA;
# MAGIC CREATE TABLE IF NOT EXISTS meta.check_result (
# MAGIC   run_id STRING, batch_id STRING, check_id STRING, check_type STRING, table_name STRING,
# MAGIC   grain_value STRING, expected DECIMAL(38,4), actual DECIMAL(38,4), threshold DECIMAL(38,4),
# MAGIC   severity STRING, status STRING, sample_keys STRING, checked_at_utc TIMESTAMP
# MAGIC ) USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE VIEW meta.v_watermark AS
# MAGIC SELECT source_system, entity, max(watermark_value) AS watermark_value,
# MAGIC        max_by(source_checksum, delivered_at_utc) AS source_checksum
# MAGIC FROM meta.ingestion_batch WHERE status = 'LOADED'
# MAGIC GROUP BY source_system, entity;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Shared quarantine and discovery views

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS slv.quarantine (
# MAGIC   stage STRING, entity STRING, _batch_id STRING, delivery_id STRING, business_key STRING,
# MAGIC   source_updated_at_utc TIMESTAMP, dq_reason STRING, row_json STRING,
# MAGIC   _run_id STRING, quarantined_at_utc TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC CREATE OR REPLACE VIEW slv.v_quarantine_open AS SELECT * FROM slv.quarantine;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SHOW TABLES IN meta;

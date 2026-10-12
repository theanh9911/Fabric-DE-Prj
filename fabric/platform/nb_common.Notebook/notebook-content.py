# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
# META   "dependencies": {}
# META }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # nb_common
# Shared runtime helpers. Import through `%run nb_common` in the processing notebook.

# CELL ********************

import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)

def safe_table_name(name):
    if not re.fullmatch(r"[a-zA-Z_][\w]*\.[a-zA-Z_][\w]*", name):
        raise ValueError(f"Unsafe table name: {name}")
    return name

def table_exists(name):
    try:
        spark.table(name).limit(0).count()
        return True
    except Exception:
        return False

def file_exists(path):
    try:
        notebookutils.fs.getFileStatus(path)
        return True
    except Exception:
        return False

def read_manifest(path):
    with notebookutils.fs.open(path, "r") as stream:
        return json.loads(stream.read())

def list_delivery_manifests(batch_from=None, batch_to=None):
    root = "Files/landing"
    found = []
    if not file_exists(root):
        return found
    for batch_dir in notebookutils.fs.ls(root):
        batch_id = batch_dir.name.rstrip("/")
        if batch_from and batch_id < batch_from:
            continue
        if batch_to and batch_id > batch_to:
            continue
        for source_dir in notebookutils.fs.ls(batch_dir.path):
            for entity_dir in notebookutils.fs.ls(source_dir.path):
                for delivery_dir in notebookutils.fs.ls(entity_dir.path):
                    manifest_path = delivery_dir.path.rstrip("/") + "/_manifest.json"
                    if file_exists(manifest_path):
                        manifest = read_manifest(manifest_path)
                        manifest["landing_path"] = delivery_dir.path.rstrip("/")
                        found.append(manifest)
    return sorted(found, key=lambda x: (x["batch_id"], x["source_system"], x["entity"],
                                        x.get("delivered_at_utc", ""), x["delivery_id"]))

def read_delivery(manifest):
    path = manifest["landing_path"]
    if manifest.get("format") == "parquet":
        df = spark.read.parquet(path)
        source_files = [f.path for f in notebookutils.fs.ls(path)
                        if f.name.lower().endswith(".parquet")]
        return df.withColumn("_source_file", F.input_file_name()).withColumn(
            "_row_no", F.monotonically_increasing_id())
    files = [f.path for f in notebookutils.fs.ls(path)
             if f.name.lower().endswith((".csv", ".txt"))]
    if not files:
        return spark.createDataFrame([], StructType([]))
    # Preserve file identity and stable-enough per-delivery row order for deterministic tie breaks.
    return (spark.read.option("header", True).option("multiLine", True)
            .option("escape", '"').option("mode", "PERMISSIVE").csv(files)
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_row_no", F.monotonically_increasing_id()))

def ensure_source_table(source):
    table = safe_table_name(f"slv.slv_{source['source_system']}_{source['entity']}")
    return table

def log_table_run(run_id, layer, table_name, event, batch_from=None, batch_to=None,
                  run_mode="normal", metrics=None, source_versions=None, error=None):
    metrics = metrics or {}
    schema = ("run_id string, batch_from string, batch_to string, run_mode string, layer string, "
              "table_name string, event string, rows_in long, rows_inserted long, rows_updated long, "
              "rows_deleted long, rows_quarantined long, delta_version long, "
              "source_versions map<string,bigint>, event_at_utc timestamp, error_message string")
    row = [(run_id, batch_from, batch_to, run_mode, layer, table_name, event,
            metrics.get("rows_in"), metrics.get("rows_inserted"), metrics.get("rows_updated"),
            metrics.get("rows_deleted"), metrics.get("rows_quarantined"),
            metrics.get("delta_version"), source_versions or {}, utc_now(), error)]
    (spark.createDataFrame(row, schema).write.format("delta").mode("append").saveAsTable("meta.table_run"))

def log_check(run_id, batch_id, check_id, check_type, table_name, grain_value,
              expected, actual, threshold=0, severity="critical", sample_keys=None):
    passed = actual is not None and expected is not None and abs(float(actual)-float(expected)) <= float(threshold)
    status = "PASS" if passed else ("WARN" if severity == "warning" else "FAIL")
    schema = ("run_id string, batch_id string, check_id string, check_type string, table_name string, "
              "grain_value string, expected decimal(38,4), actual decimal(38,4), threshold decimal(38,4), "
              "severity string, status string, sample_keys string, checked_at_utc timestamp")
    row = [(run_id, batch_id, check_id, check_type, table_name, grain_value,
            expected, actual, threshold, severity, status, sample_keys, utc_now())]
    spark.createDataFrame(row, schema).write.format("delta").mode("append").saveAsTable("meta.check_result")
    if status == "FAIL" and severity == "critical":
        raise ValueError(f"Critical check failed: {check_id}, expected={expected}, actual={actual}")
    return status

def append_ingestion_record(manifest, run_id, status, metrics=None, error=None):
    metrics = metrics or {}
    schema = ("batch_id string, source_system string, entity string, delivery_id string, "
              "delivered_at_utc timestamp, load_strategy string, window_start string, window_end string, "
              "landing_path string, files_written int, rows_copied long, source_checksum string, "
              "rows_read long, rows_changed long, rows_unchanged long, rows_quarantined long, "
              "watermark_value string, schema_changes string, status string, loaded_run_id string, "
              "loaded_at_utc timestamp, error_message string")
    delivered = manifest.get("delivered_at_utc")
    try:
        delivered = datetime.fromisoformat(delivered.replace("Z", "+00:00")).replace(tzinfo=None) if delivered else utc_now()
    except Exception:
        delivered = utc_now()
    row = [(manifest["batch_id"], manifest["source_system"], manifest["entity"],
            manifest["delivery_id"], delivered, manifest.get("load_strategy"),
            manifest.get("window_start"), manifest.get("window_end"), manifest["landing_path"],
            int(manifest.get("files_written", 0)), int(manifest.get("rows_copied") or 0),
            manifest.get("source_checksum"), metrics.get("rows_read"), metrics.get("rows_changed"),
            metrics.get("rows_unchanged"), metrics.get("rows_quarantined"),
            metrics.get("watermark_value"), json.dumps(manifest.get("schema_changes", [])),
            status, run_id, utc_now(), error)]
    (spark.createDataFrame(row, schema).write.format("delta").mode("append")
     .saveAsTable("meta.ingestion_batch"))

def already_loaded(manifest):
    if not table_exists("meta.ingestion_batch"):
        return False
    return (spark.table("meta.ingestion_batch")
            .where((F.col("batch_id") == manifest["batch_id"]) &
                   (F.col("source_system") == manifest["source_system"]) &
                   (F.col("entity") == manifest["entity"]) &
                   (F.col("delivery_id") == manifest["delivery_id"]) &
                   (F.col("status") == "LOADED")).limit(1).count() > 0)

def merge_current(table_name, source_df, keys, order_col="_updated_at_utc"):
    table_name = safe_table_name(table_name)
    source_df.createOrReplaceTempView("_merge_src")
    if not table_exists(table_name):
        (source_df.write.format("delta").option("delta.enableChangeDataFeed", "true")
         .mode("overwrite").saveAsTable(table_name))
        spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed=true)")
        return source_df.count()
    condition = " AND ".join([f"t.`{k}` = s.`{k}`" for k in keys])
    cols = source_df.columns
    assignments = ", ".join([f"`{c}` = s.`{c}`" for c in cols])
    insert_cols = ", ".join([f"`{c}`" for c in cols])
    insert_vals = ", ".join([f"s.`{c}`" for c in cols])
    newer = f"s.`{order_col}` > t.`{order_col}`" if order_col in cols else "false"
    if "_delivered_at_utc" in cols:
        newer += (f" OR (s.`{order_col}` = t.`{order_col}` AND "
                  "s.`_delivered_at_utc` > t.`_delivered_at_utc`)")
    spark.sql(f"MERGE INTO {table_name} t USING _merge_src s ON {condition} "
              f"WHEN MATCHED AND ({newer}) THEN UPDATE SET {assignments} "
              f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})")
    return source_df.count()

def append_only_merge(table_name, source_df, keys):
    table_name = safe_table_name(table_name)
    if not table_exists(table_name):
        (source_df.write.format("delta").option("delta.enableChangeDataFeed", "true")
         .mode("overwrite").saveAsTable(table_name))
        spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed=true)")
        return source_df.count()
    source_df.createOrReplaceTempView("_append_src")
    condition = " AND ".join([f"t.`{k}` <=> s.`{k}`" for k in keys])
    cols = source_df.columns
    insert_cols = ", ".join([f"`{c}`" for c in cols])
    insert_vals = ", ".join([f"s.`{c}`" for c in cols])
    spark.sql(f"MERGE INTO {table_name} t USING _append_src s ON {condition} "
              f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})")
    return source_df.count()

def write_quarantine(df, stage, entity, batch_id, run_id, reason_col="_dq_reason",
                     business_key_col="_bk", updated_col="_updated_at_utc", delivery_id=None):
    if df.limit(1).count() == 0:
        return 0
    cols = [c for c in df.columns if not c.startswith("_")]
    result = (df.withColumn("stage", F.lit(stage)).withColumn("entity", F.lit(entity))
              .withColumn("_batch_id", F.lit(batch_id))
              .withColumn("delivery_id", F.lit(delivery_id))
              .withColumn("business_key", F.col(business_key_col).cast("string") if business_key_col in df.columns else F.lit(None).cast("string"))
              .withColumn("source_updated_at_utc", F.col(updated_col).cast("timestamp") if updated_col in df.columns else F.lit(None).cast("timestamp"))
              .withColumn("dq_reason", F.col(reason_col).cast("string"))
              .withColumn("row_json", F.to_json(F.struct(*[F.col(c) for c in cols])))
              .withColumn("_run_id", F.lit(run_id)).withColumn("quarantined_at_utc", F.current_timestamp())
              .select("stage", "entity", "_batch_id", "delivery_id", "business_key", "source_updated_at_utc",
                      "dq_reason", "row_json", "_run_id", "quarantined_at_utc"))
    append_only_merge("slv.quarantine", result,
                      ["stage", "entity", "_batch_id", "delivery_id", "business_key", "dq_reason", "row_json"])
    return result.count()

def resolve_quarantine_batch(stage, entity, batch_id, delivery_id=None):
    if table_exists("slv.quarantine"):
        delivery_filter = f" AND delivery_id = '{delivery_id}'" if delivery_id else ""
        spark.sql("DELETE FROM slv.quarantine "
                  f"WHERE stage = '{stage}' AND entity = '{entity}' AND _batch_id = '{batch_id}'{delivery_filter}")

def resolve_quarantine_keys(stage, entity, key_df):
    if not table_exists("slv.quarantine") or key_df.limit(1).count() == 0:
        return
    key_df.select(F.col("_bk").cast("string").alias("_resolved_key")).distinct().createOrReplaceTempView("_resolved_quarantine_keys")
    spark.sql("DELETE FROM slv.quarantine "
              f"WHERE stage = '{stage}' AND entity = '{entity}' "
              "AND business_key IN (SELECT _resolved_key FROM _resolved_quarantine_keys)")

def source_table(source):
    return safe_table_name(f"slv.slv_{source['source_system']}_{source['entity']}")

def validate_config(sources, tables):
    source_keys = {(s["source_system"], s["entity"]) for s in sources}
    if len(source_keys) != len(sources):
        raise ValueError("Duplicate source/entity configuration")
    names = {t["table_name"] for t in tables}
    known = names | {source_table(s) for s in sources}
    for table in tables:
        unknown = set(table.get("depends_on", [])) - known
        if unknown:
            raise ValueError(f"Unknown dependencies for {table['table_name']}: {unknown}")
    return True

def topological_order(nodes):
    pending = {n["name"]: dict(n) for n in nodes}
    ordered = []
    while pending:
        ready = [name for name, n in pending.items() if set(n.get("deps", [])) <= {x["name"] for x in ordered}]
        if not ready:
            raise ValueError(f"DAG has cycle or missing dependency: {sorted(pending)}")
        for name in sorted(ready):
            ordered.append(pending.pop(name))
    return ordered

def run_dag(nodes, params=None):
    params = params or {}
    results = {}
    for node in topological_order(nodes):
        name = node["name"]
        if not spark.catalog.tableExists(node["notebook"]):
            raise ValueError(f"Notebook dependency not attached/imported: {node['notebook']}")
        args = {**params, **node.get("args", {})}
        results[name] = notebookutils.notebook.run(node["notebook"], 3600, args)
    return results

def table_version(table_name):
    if not table_exists(table_name):
        return -1
    row = spark.sql(f"DESCRIBE HISTORY {safe_table_name(table_name)} LIMIT 1").first()
    return int(row["version"]) if row else -1

def source_versions(table_names):
    return {name: table_version(name) for name in table_names}

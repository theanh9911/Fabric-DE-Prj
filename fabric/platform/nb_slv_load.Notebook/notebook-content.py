# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

p_run_id = ""
p_mode = "normal"
p_load_date = ""
p_load_date_to = ""
p_entities = "ALL"
p_full_reload = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_config
%run nb_common

import json
from pyspark.sql import functions as F
from pyspark.sql.window import Window

run_id = p_run_id or str(uuid.uuid4())
batch_from = p_load_date.replace("-", "") if p_load_date else None
batch_to = p_load_date_to.replace("-", "") if p_load_date_to else batch_from
requested = None if p_entities == "ALL" else {x.strip() for x in p_entities.split(",")}
source_specs = [s for s in SOURCES if s.get("is_active", True) and
                (requested is None or f"{s['source_system']}.{s['entity']}" in requested)]

all_manifests = list_delivery_manifests(batch_from, batch_to)
source_keys = {(s["source_system"], s["entity"]): s for s in source_specs}
pending = [m for m in all_manifests if (m["source_system"], m["entity"]) in source_keys and
           (p_mode == "rerun" or not already_loaded(m))]
pending.sort(key=lambda m: (source_keys[(m["source_system"], m["entity"])]["load_order"],
                            m["batch_id"], m.get("delivered_at_utc", "")))

loaded_sources = set()
errors = []
for manifest in pending:
    spec = source_keys[(manifest["source_system"], manifest["entity"])]
    entity_name = f"{spec['source_system']}.{spec['entity']}"
    target = ensure_source_table(spec)
    log_table_run(run_id, "slv", target, "STARTED", batch_from, batch_to, p_mode)
    try:
        df = read_delivery(manifest)
        if len(df.columns) == 0:
            append_ingestion_record(manifest, run_id, "LOADED", {"rows_read": 0, "rows_changed": 0,
                                                                   "rows_unchanged": 0, "rows_quarantined": 0})
            continue
        business_cols = [c for c in df.columns if not c.startswith("_")]
        required_keys = spec.get("business_keys", [])
        missing = [c for c in required_keys if c not in business_cols]
        if missing:
            raise ValueError(f"Missing business key columns {missing}; got {business_cols}")
        if "source_system" not in business_cols:
            df = df.withColumn("source_system", F.lit(spec["source_system"]))
            business_cols = business_cols + ["source_system"]
        if "updated_at" not in business_cols:
            if spec["kind"] == "event":
                raise ValueError(f"Event source {entity_name} has no updated_at")
            df = df.withColumn("updated_at", F.lit(manifest.get("batch_id")).cast("timestamp"))
        df = (df.withColumn("_batch_id", F.lit(manifest["batch_id"]))
              .withColumn("_delivery_id", F.lit(manifest["delivery_id"]))
              .withColumn("_delivered_at_utc", F.to_timestamp(F.lit(manifest.get("delivered_at_utc"))))
              .withColumn("_run_id", F.lit(run_id)).withColumn("_ingested_at", F.current_timestamp()))
        for key in required_keys:
            df = df.withColumn(key, F.col(key).cast("string"))
        norm_keys = [F.upper(F.regexp_replace(F.trim(F.col(k)), r"\s+", "")) for k in required_keys]
        df = df.withColumn("_bk", F.concat_ws("|", *norm_keys))
        if spec["kind"] == "event":
            df = df.withColumn("_updated_at_utc", F.to_utc_timestamp(F.to_timestamp("updated_at"), spec.get("source_timezone", "UTC")))
        else:
            df = df.withColumn("_updated_at_utc", F.coalesce(
                F.to_utc_timestamp(F.to_timestamp("updated_at"), spec.get("source_timezone", "UTC")),
                F.col("_delivered_at_utc")))
        df = df.withColumn("_snapshot_date", F.to_date(F.lit(manifest["batch_id"]), "yyyyMMdd"))
        df = df.withColumn("is_deleted", F.lit(False))
        hash_cols = [F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in business_cols]
        df = df.withColumn("_record_hash", F.sha2(F.concat_ws("\u001f", *hash_cols), 256))
        missing_key = reduce(lambda a, b: a | b, [F.col(k).isNull() | (F.trim(F.col(k).cast("string")) == "") for k in required_keys])
        invalid_ts = F.col("_updated_at_utc").isNull() if spec["kind"] == "event" else F.lit(False)
        invalid = df.withColumn("_dq_reason", F.when(missing_key, F.lit("missing_business_key"))
                                .when(invalid_ts, F.lit("invalid_updated_at")))
        bad = invalid.filter(F.col("_dq_reason").isNotNull())
        quarantined = bad.count()
        good = invalid.filter(F.col("_dq_reason").isNull()).drop("_dq_reason")
        if spec["kind"] == "event":
            event_rows = good
            append_only_merge("slv.slv_orders_event", event_rows,
                              ["source_system", "_bk", "order_status", "_updated_at_utc"])
        else:
            snapshot_events = good.select(
                F.lit(spec["source_system"]).alias("source_system"),
                F.lit(spec["entity"]).alias("entity"), "_bk", "_snapshot_date", "_record_hash",
                "_batch_id", "_delivery_id", "_delivered_at_utc",
                *(["_source_file"] if "_source_file" in good.columns else []),
                *(["_row_no"] if "_row_no" in good.columns else []), "_run_id", "_ingested_at",
                *[c for c in good.columns if not c.startswith("_") and c != "source_system"])
            append_only_merge("slv.slv_snapshot_event", snapshot_events,
                              ["source_system", "entity", "_bk", "_snapshot_date", "_record_hash"])
        tie_cols = [F.col("_updated_at_utc").desc_nulls_last(), F.col("_delivered_at_utc").desc_nulls_last(),
                    F.col("_source_file").desc_nulls_last() if "_source_file" in good.columns else F.lit(0),
                    F.col("_row_no").desc_nulls_last() if "_row_no" in good.columns else F.lit(0)]
        w = Window.partitionBy("_bk").orderBy(*tie_cols)
        good = good.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
        if spec["kind"] == "event":
            merge_current(target, good, ["_bk"])
        else:
            merge_current(target, good, ["_bk"], "_snapshot_date")
            if manifest.get("is_latest_snapshot"):
                target_df = spark.table(target)
                current_keys = good.select("_bk").distinct()
                snapshot_day = F.to_date(F.lit(manifest["batch_id"]), "yyyyMMdd")
                missing = (target_df.join(current_keys, "_bk", "left_anti")
                           .where((~F.col("is_deleted")) & (F.col("_snapshot_date") < snapshot_day)))
                if missing.limit(1).count():
                    missing = (missing.withColumn("_snapshot_date", snapshot_day)
                               .withColumn("is_deleted", F.lit(True)).withColumn("_run_id", F.lit(run_id))
                               .withColumn("_delivery_id", F.lit(manifest["delivery_id"]))
                               .withColumn("_delivered_at_utc", F.to_timestamp(F.lit(manifest.get("delivered_at_utc"))))
                               .withColumn("_ingested_at", F.current_timestamp()))
                    tombstones = missing.select(
                        F.lit(spec["source_system"]).alias("source_system"),
                        F.lit(spec["entity"]).alias("entity"), "_bk", "_snapshot_date", "_record_hash",
                        "_batch_id", "_delivery_id", "_delivered_at_utc",
                        *(["_source_file"] if "_source_file" in missing.columns else []),
                        *(["_row_no"] if "_row_no" in missing.columns else []), "_run_id", "_ingested_at",
                        *[c for c in missing.columns if not c.startswith("_") and c not in ("source_system", "is_deleted")],
                        "is_deleted")
                    append_only_merge("slv.slv_snapshot_event", tombstones,
                                      ["source_system", "entity", "_bk", "_snapshot_date", "is_deleted"])
                    missing.createOrReplaceTempView("_missing_snapshot_keys")
                    spark.sql(f"MERGE INTO {target} t USING _missing_snapshot_keys s ON t._bk=s._bk "
                              "WHEN MATCHED THEN UPDATE SET t.is_deleted=true, t._snapshot_date=s._snapshot_date, "
                              "t._run_id=s._run_id")
        # Replace earlier technical quarantine for this replayed delivery only after the data commit succeeds.
        resolve_quarantine_batch("slv", entity_name, manifest["batch_id"], manifest["delivery_id"])
        write_quarantine(bad, "slv", entity_name, manifest["batch_id"], run_id,
                         delivery_id=manifest["delivery_id"])
        rows_read = df.count()
        rows_changed = good.count()
        append_ingestion_record(manifest, run_id, "LOADED", {
            "rows_read": rows_read, "rows_changed": rows_changed,
            "rows_unchanged": max(rows_read - rows_changed - quarantined, 0),
            "rows_quarantined": quarantined,
            "watermark_value": manifest.get("window_end") or manifest.get("watermark_value")})
        loaded_sources.add(entity_name)
        log_table_run(run_id, "slv", target, "SUCCEEDED", batch_from, batch_to, p_mode,
                      {"rows_in": rows_read, "rows_inserted": rows_changed,
                       "rows_quarantined": quarantined}, source_versions={target: table_version(target)})
    except Exception as exc:
        append_ingestion_record(manifest, run_id, "FAILED", error=str(exc))
        log_table_run(run_id, "slv", target, "FAILED", batch_from, batch_to, p_mode, error=str(exc))
        errors.append(f"{entity_name}/{manifest['delivery_id']}: {exc}")

if errors:
    raise RuntimeError("Silver delivery failures: " + "; ".join(errors))
notebookutils.notebook.exit(json.dumps({"run_id": run_id, "loaded": sorted(loaded_sources),
                                        "delivery_count": len(pending)}))

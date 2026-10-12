# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

p_run_id = ""
p_load_date = ""
p_load_date_to = ""
p_mode = "normal"
p_from_layer = "ingest"
p_entities = "ALL"
p_full_reload = False
p_source_workspace = "CompanyA-Source"
p_erp_item = "sqldb_erp_wholesale"
p_drop_lakehouse = "lh_retail_drop"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


%run nb_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


%run nb_common

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from pyspark.sql import functions as F

def run_child_notebook(name, timeout_seconds, arguments):
    # Pass the current workspace Lakehouse through even if child binding metadata differs.
    return notebookutils.notebook.run(name, timeout_seconds, {
        **arguments, "useRootDefaultLakehouse": True})

def run_audited_notebook(name, timeout_seconds, arguments, layer, audit_name,
                         batch_from=None, batch_to=None, run_mode="normal",
                         validate_result=None, metrics_from_result=None,
                         source_versions=None):
    log_table_run(run_id, layer, audit_name, "STARTED", batch_from, batch_to, run_mode)
    try:
        raw_result = run_child_notebook(name, timeout_seconds, arguments)
        result = json.loads(raw_result) if isinstance(raw_result, str) else raw_result
        if validate_result:
            validate_result(result)
        metrics = metrics_from_result(result) if metrics_from_result else {}
        log_table_run(run_id, layer, audit_name, "SUCCEEDED", batch_from, batch_to,
                      run_mode, metrics=metrics, source_versions=source_versions)
        return result
    except Exception as exc:
        log_table_run(run_id, layer, audit_name, "FAILED", batch_from, batch_to,
                      run_mode, error=str(exc))
        log_table_run(run_id, "pipeline", "pipeline.run_process", "FAILED",
                      batch_from, batch_to, run_mode,
                      error=f"Child notebook {name} failed: {exc}")
        raise

run_id = p_run_id or str(uuid.uuid4())
batch_from = p_load_date.replace("-", "") if p_load_date else None
batch_to = p_load_date_to.replace("-", "") if p_load_date_to else batch_from
ingest_error = None
log_table_run(run_id, "pipeline", "pipeline.run_process", "STARTED",
              batch_from, batch_to, p_mode)

# One pipeline Notebook activity owns the Spark session. Nested notebooks inherit its pool.
if p_mode == "normal" and p_from_layer == "ingest":
    plan_doc = run_audited_notebook("nb_ingest_plan", 900, {
        "p_run_id": run_id,
        "p_load_date": p_load_date, "p_window_start": "", "p_window_end": "",
        "p_entities": p_entities, "p_mode": p_mode, "p_from_layer": p_from_layer},
        "ingest", "pipeline.ingest_plan", batch_from, batch_to, p_mode,
        metrics_from_result=lambda result: {"rows_in": len(result.get("sources", []))})
    p_load_date = plan_doc["load_date"]
    batch_from = plan_doc["batch_id"]
    batch_to = p_load_date_to.replace("-", "") if p_load_date_to else batch_from
    run_id = plan_doc["run_id"]
    if plan_doc.get("sources"):
        for source in plan_doc["sources"]:
            log_table_run(run_id, "ingest",
                          f"{source.get('source_system')}.{source.get('entity')}",
                          "STARTED", batch_from, batch_to, p_mode)
        try:
            ingest_result = run_audited_notebook("nb_ingest", 3600, {
                "p_plan_json": json.dumps(plan_doc),
                "p_source_workspace": p_source_workspace,
                "p_erp_item": p_erp_item,
                "p_drop_lakehouse": p_drop_lakehouse},
                "ingest", "pipeline.ingest", batch_from, batch_to, p_mode,
                metrics_from_result=lambda result: {
                    "rows_in": len(result.get("source_results", [])),
                    "rows_inserted": len(result.get("deliveries", []))})
            for source_result in ingest_result.get("source_results", []):
                outcome = source_result.get("outcome", "UNKNOWN")
                event = {"DELIVERED": "SUCCEEDED", "NO_DATA": "NO_DATA",
                         "UNCHANGED": "UNCHANGED", "SKIPPED": "SKIPPED"}.get(outcome, "FAILED")
                log_table_run(run_id, "ingest",
                              f"{source_result.get('source_system')}.{source_result.get('entity')}",
                              event, batch_from, batch_to, p_mode,
                              metrics={"rows_in": source_result.get("rows_read"),
                                       "rows_inserted": 1 if outcome == "DELIVERED" else 0},
                              error={"NO_DATA": "Source returned no rows or files",
                                     "UNCHANGED": "Snapshot checksum matched the prior loaded snapshot",
                                     "SKIPPED": source_result.get("reason")}.get(outcome))
        except Exception as exc:
            # Preserve earlier deliveries, but retain an explicit failed stage event.
            ingest_error = str(exc)
            for source in plan_doc["sources"]:
                log_table_run(run_id, "ingest",
                              f"{source.get('source_system')}.{source.get('entity')}",
                              "INCOMPLETE", batch_from, batch_to, p_mode,
                              error="Ingest stopped before returning per-source outcomes; inspect Landing manifests")
    else:
        log_table_run(run_id, "ingest", "pipeline.ingest", "SKIPPED",
                      batch_from, batch_to, p_mode, error="Planner returned no active sources")
else:
    reason = f"Ingest is not selected for mode={p_mode}, from_layer={p_from_layer}"
    log_table_run(run_id, "ingest", "pipeline.ingest_plan", "SKIPPED",
                  batch_from, batch_to, p_mode, error=reason)
    log_table_run(run_id, "ingest", "pipeline.ingest", "SKIPPED",
                  batch_from, batch_to, p_mode, error=reason)

source_selected = [s for s in SOURCES if s.get("is_active", True)]
if p_entities != "ALL":
    requested = {x.strip() for x in p_entities.split(",")}
    source_selected = [s for s in source_selected if f"{s['source_system']}.{s['entity']}" in requested]

loaded = []
try:
    manifests = list_delivery_manifests(batch_from, batch_to)
    pending = [m for m in manifests
               if (p_mode == "rerun" or not already_loaded(m)) and
               any(s["source_system"] == m["source_system"] and s["entity"] == m["entity"]
                   for s in source_selected)]
except Exception as exc:
    log_table_run(run_id, "pipeline", "pipeline.delivery_discovery", "FAILED",
                  batch_from, batch_to, p_mode, error=str(exc))
    log_table_run(run_id, "pipeline", "pipeline.run_process", "FAILED",
                  batch_from, batch_to, p_mode, error=f"Landing manifest discovery failed: {exc}")
    raise
if p_from_layer in ("ingest", "slv"):
    if pending:
        result = run_audited_notebook("nb_slv_load", 7200, {
            "p_run_id": run_id, "p_mode": p_mode, "p_load_date": p_load_date,
            "p_load_date_to": p_load_date_to, "p_entities": p_entities,
            "p_full_reload": p_full_reload},
            "slv", "pipeline.silver", batch_from, batch_to, p_mode,
            metrics_from_result=lambda result: {"rows_in": result.get("delivery_count", 0),
                                                "rows_inserted": len(result.get("loaded", []))})
        loaded.append(result)
    else:
        log_table_run(run_id, "slv", "pipeline.silver", "NO_DATA",
                      batch_from, batch_to, p_mode, metrics={"rows_in": 0, "rows_inserted": 0},
                      error="No pending Landing manifests matched the selected sources and batch range")
else:
    log_table_run(run_id, "slv", "pipeline.silver", "SKIPPED",
                  batch_from, batch_to, p_mode, error=f"Run started from layer {p_from_layer}")

gold_requested = [t for t in TABLES if t["table_name"].startswith("gld.")]
if p_entities != "ALL":
    gold_entities = {x.strip() for x in p_entities.split(",") if x.strip().startswith("gld.")}
    if gold_entities:
        gold_requested = [t for t in gold_requested if t["table_name"] in gold_entities]

gold_result = None
upstream_tables = sorted({source_table(s) for s in SOURCES if table_exists(source_table(s))})
current_versions = source_versions(upstream_tables)
previous_gold = None
if table_exists("meta.table_run"):
    rows = (spark.table("meta.table_run").where((F.col("layer") == "gld") &
             (F.col("table_name") == "gld.gld_f_sales_line") & (F.col("event") == "SUCCEEDED"))
            .orderBy(F.col("event_at_utc").desc()).limit(1).collect())
    previous_gold = rows[0]["source_versions"] if rows else None
gold_changed = (p_mode == "rerun" or p_full_reload or p_from_layer == "gld" or
                previous_gold is None or
                any(int(current_versions.get(k, -1)) != int((previous_gold or {}).get(k, -2))
                    for k in current_versions))

if gold_changed and p_from_layer in ("ingest", "slv", "gld"):
    log_table_run(run_id, "gld", "pipeline.gold", "STARTED", batch_from, batch_to, p_mode)
    try:
        # Deterministic rebuild from Silver current + append-only snapshot history.
        run_audited_notebook("nb_gld_dims", 7200, {
            "p_run_id": run_id, "p_entities": p_entities, "p_full_reload": p_full_reload},
            "gld", "pipeline.gold_dimensions", batch_from, batch_to, p_mode)

        def require_fact_result(result):
            if result.get("reason") == "no Silver orders":
                raise RuntimeError("Gold fact cannot run: no Silver orders tables exist. "
                                   "Check Landing deliveries and nb_slv_load audit events.")

        gold_result = run_audited_notebook("nb_gld_fact", 7200, {
            "p_run_id": run_id, "p_batch_from": batch_from or "", "p_batch_to": batch_to or "",
            "p_mode": p_mode, "p_full_reload": p_full_reload},
            "gld", "gld.gld_f_sales_line", batch_from, batch_to, p_mode,
            validate_result=require_fact_result,
            metrics_from_result=lambda result: {"rows_in": result.get("fact_rows", 0),
                                                "rows_quarantined": result.get("quarantined", 0)},
            source_versions=current_versions)
    except Exception as exc:
        log_table_run(run_id, "gld", "pipeline.gold", "FAILED", batch_from, batch_to,
                      p_mode, source_versions=current_versions, error=str(exc))
        raise
else:
    log_table_run(run_id, "gld", "pipeline.gold", "SKIPPED", batch_from, batch_to, p_mode,
                  source_versions=current_versions,
                  error="No upstream Silver version change and no explicit Gold rerun/full reload")

dq_result = run_audited_notebook("nb_dq", 3600, {
    "p_run_id": run_id, "p_batch_id": batch_from or "", "p_layer": "all"},
    "dq", "pipeline.dq", batch_from, batch_to, p_mode,
    metrics_from_result=lambda result: {"rows_in": len(result.get("checks", [])),
                                        "rows_quarantined": result.get("quarantine_rows", 0)})
recon_result = run_audited_notebook("nb_ops_recon", 3600, {
    "p_run_id": run_id, "p_batch_id": batch_from or ""},
    "recon", "pipeline.reconciliation", batch_from, batch_to, p_mode,
    metrics_from_result=lambda result: {"rows_in": len(result.get("checks", []))})

if ingest_error:
    raise RuntimeError(f"Ingest failed after preserving completed deliveries: {ingest_error}")

# Only the master pipeline refreshes the semantic model, after this notebook succeeds.
log_table_run(run_id, "pipeline", "pipeline.run_process", "SUCCEEDED",
              batch_from, batch_to, p_mode)
notebookutils.notebook.exit(json.dumps({"run_id": run_id, "silver": loaded,
                                        "gold": gold_result, "dq": dq_result,
                                        "recon": recon_result, "gold_changed": gold_changed}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
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

# CELL ********************


%run nb_common

# CELL ********************

import json
from pyspark.sql import functions as F

def run_child_notebook(name, timeout_seconds, arguments):
    # Pass the current workspace Lakehouse through even if child binding metadata differs.
    return notebookutils.notebook.run(name, timeout_seconds, {
        **arguments, "useRootDefaultLakehouse": True})

run_id = p_run_id or str(uuid.uuid4())
batch_from = p_load_date.replace("-", "") if p_load_date else None
batch_to = p_load_date_to.replace("-", "") if p_load_date_to else batch_from
ingest_error = None

# One pipeline Notebook activity owns the Spark session. Nested notebooks inherit its pool.
if p_mode == "normal" and p_from_layer == "ingest":
    plan_json = run_child_notebook("nb_ingest_plan", 900, {
        "p_load_date": p_load_date, "p_window_start": "", "p_window_end": "",
        "p_entities": p_entities, "p_mode": p_mode, "p_from_layer": p_from_layer})
    plan_doc = json.loads(plan_json)
    p_load_date = plan_doc["load_date"]
    batch_from = plan_doc["batch_id"]
    batch_to = p_load_date_to.replace("-", "") if p_load_date_to else batch_from
    run_id = plan_doc["run_id"]
    if plan_doc.get("sources"):
        try:
            run_child_notebook("nb_ingest", 3600, {
                "p_plan_json": plan_json,
                "p_source_workspace": p_source_workspace,
                "p_erp_item": p_erp_item,
                "p_drop_lakehouse": p_drop_lakehouse})
        except Exception as exc:
            # Continue with deliveries that completed before the failed source; the pipeline still ends FAILED.
            ingest_error = str(exc)

source_selected = [s for s in SOURCES if s.get("is_active", True)]
if p_entities != "ALL":
    requested = {x.strip() for x in p_entities.split(",")}
    source_selected = [s for s in source_selected if f"{s['source_system']}.{s['entity']}" in requested]

loaded = []
pending = [m for m in list_delivery_manifests(batch_from, batch_to)
           if (p_mode == "rerun" or not already_loaded(m)) and
           any(s["source_system"] == m["source_system"] and s["entity"] == m["entity"] for s in source_selected)]
if p_from_layer in ("ingest", "slv") and pending:
    result = run_child_notebook("nb_slv_load", 7200, {
        "p_run_id": run_id, "p_mode": p_mode, "p_load_date": p_load_date,
        "p_load_date_to": p_load_date_to, "p_entities": p_entities,
        "p_full_reload": p_full_reload})
    loaded.append(json.loads(result) if isinstance(result, str) and result.startswith("{") else result)

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
    log_table_run(run_id, "gld", "gld.gld_f_sales_line", "STARTED", batch_from, batch_to, p_mode)
    try:
        # Deterministic rebuild from Silver current + append-only snapshot history.
        run_child_notebook("nb_gld_dims", 7200, {
            "p_run_id": run_id, "p_entities": p_entities, "p_full_reload": p_full_reload})
        gold_result = run_child_notebook("nb_gld_fact", 7200, {
            "p_run_id": run_id, "p_batch_from": batch_from or "", "p_batch_to": batch_to or "",
            "p_mode": p_mode, "p_full_reload": p_full_reload})
        log_table_run(run_id, "gld", "gld.gld_f_sales_line", "SUCCEEDED", batch_from, batch_to,
                      p_mode, source_versions=current_versions)
    except Exception as exc:
        log_table_run(run_id, "gld", "gld.gld_f_sales_line", "FAILED", batch_from, batch_to,
                      p_mode, source_versions=current_versions, error=str(exc))
        raise

dq_result = run_child_notebook("nb_dq", 3600, {
    "p_run_id": run_id, "p_batch_id": batch_from or "", "p_layer": "all"})
recon_result = run_child_notebook("nb_ops_recon", 3600, {
    "p_run_id": run_id, "p_batch_id": batch_from or ""})

if ingest_error:
    raise RuntimeError(f"Ingest failed after preserving completed deliveries: {ingest_error}")

# Only the master pipeline refreshes the semantic model, after this notebook succeeds.
notebookutils.notebook.exit(json.dumps({"run_id": run_id, "silver": loaded,
                                        "gold": gold_result, "dq": dq_result,
                                        "recon": recon_result, "gold_changed": gold_changed}))

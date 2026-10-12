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

def validate_ingest_result(result, planned_sources, batch_from, batch_to):
    """Require one explicit terminal outcome for every planned source entity."""
    source_results = result.get("source_results")
    if not isinstance(source_results, list):
        raise RuntimeError("nb_ingest contract error: source_results must be a list")
    expected = {(s["source_system"], s["entity"]) for s in planned_sources}
    actual = [(r.get("source_system"), r.get("entity")) for r in source_results]
    duplicates = sorted({key for key in actual if actual.count(key) > 1})
    missing = sorted(expected - set(actual))
    unexpected = sorted(set(actual) - expected)
    if duplicates or missing or unexpected:
        raise RuntimeError(
            "nb_ingest returned incomplete per-source outcomes: "
            f"missing={missing}, duplicate={duplicates}, unexpected={unexpected}")
    allowed = {"DELIVERED", "NO_DATA", "UNCHANGED", "SKIPPED"}
    invalid = [(r.get("source_system"), r.get("entity"), r.get("outcome"))
               for r in source_results if r.get("outcome") not in allowed]
    if invalid:
        raise RuntimeError(f"nb_ingest returned invalid source outcomes: {invalid}")
    delivered = {(r.get("source_system"), r.get("entity")) for r in source_results
                 if r.get("outcome") == "DELIVERED"}
    manifests = {(m.get("source_system"), m.get("entity"))
                 for m in result.get("deliveries", [])}
    if delivered != manifests:
        raise RuntimeError(
            "nb_ingest delivery/source outcome mismatch: "
            f"delivered outcomes without manifests={sorted(delivered - manifests)}, "
            f"manifests without delivered outcomes={sorted(manifests - delivered)}")
    expected_delivery_ids = {m.get("delivery_id") for m in result.get("deliveries", [])}
    visible_delivery_ids = {m.get("delivery_id")
                            for m in list_delivery_manifests(batch_from, batch_to)}
    invisible = sorted(x for x in expected_delivery_ids if x not in visible_delivery_ids)
    if invisible:
        raise RuntimeError(
            "nb_ingest returned deliveries that are not visible to nb_process in "
            f"the bound platform Lakehouse (batch {batch_from}-{batch_to}): {invisible}. "
            "Check that child notebooks inherit the same default Lakehouse binding.")

run_id = p_run_id or str(uuid.uuid4())
batch_from = p_load_date.replace("-", "") if p_load_date else None
batch_to = p_load_date_to.replace("-", "") if p_load_date_to else batch_from
ingest_error = None
log_table_run(run_id, "pipeline", "pipeline.run_process", "STARTED",
              batch_from, batch_to, p_mode)

try:
    valid_layers = {"ingest", "slv", "gld"}
    if p_from_layer not in valid_layers:
        raise ValueError(f"Invalid p_from_layer={p_from_layer}; expected one of {sorted(valid_layers)}")
    if p_mode not in {"normal", "rerun"}:
        raise ValueError(f"Invalid p_mode={p_mode}; expected 'normal' or 'rerun'")
    for parameter_name, parameter_value in (("p_load_date", p_load_date),
                                             ("p_load_date_to", p_load_date_to)):
        if parameter_value:
            try:
                datetime.strptime(parameter_value, "%Y-%m-%d")
            except ValueError as exc:
                raise ValueError(f"{parameter_name} must use YYYY-MM-DD format") from exc
    if p_load_date and p_load_date_to and p_load_date_to < p_load_date:
        raise ValueError("p_load_date_to must be the same as or later than p_load_date")
    if p_load_date_to and not p_load_date:
        raise ValueError("p_load_date is required when p_load_date_to is supplied")
    if p_entities != "ALL":
        requested_entities = {x.strip() for x in p_entities.split(",") if x.strip()}
        if not requested_entities:
            raise ValueError("p_entities must be ALL or a comma-separated list of source entities")
        valid_source_entities = {f"{s['source_system']}.{s['entity']}" for s in SOURCES}
        unknown_entities = requested_entities - valid_source_entities
        if unknown_entities:
            raise ValueError(
                "p_entities currently accepts source entities only (or ALL); "
                f"unknown values: {sorted(unknown_entities)}")
    if p_mode == "rerun" and p_from_layer == "ingest":
        raise ValueError(
            "p_mode=rerun does not read source data; use p_from_layer=slv to replay Landing, "
            "or p_mode=normal with p_from_layer=ingest to ingest from source")
except Exception as exc:
    log_table_run(run_id, "pipeline", "pipeline.run_process", "FAILED",
                  batch_from, batch_to, p_mode, error=f"Preflight validation failed: {exc}")
    raise

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
                validate_result=lambda result: validate_ingest_result(
                    result, plan_doc["sources"], batch_from, batch_to),
                metrics_from_result=lambda result: {
                    "rows_in": sum(int(r.get("rows_read") or 0)
                                   for r in result.get("source_results", [])),
                    "rows_inserted": sum(int(r.get("rows_read") or 0)
                                          for r in result.get("source_results", [])
                                          if r.get("outcome") == "DELIVERED")})
            for source_result in ingest_result.get("source_results", []):
                outcome = source_result.get("outcome", "UNKNOWN")
                event = {"DELIVERED": "SUCCEEDED", "NO_DATA": "NO_DATA",
                         "UNCHANGED": "UNCHANGED", "SKIPPED": "SKIPPED"}.get(outcome, "FAILED")
                log_table_run(run_id, "ingest",
                              f"{source_result.get('source_system')}.{source_result.get('entity')}",
                              event, batch_from, batch_to, p_mode,
                              metrics={"rows_in": source_result.get("rows_read"),
                                       "rows_inserted": (source_result.get("rows_read") or 0)
                                       if outcome == "DELIVERED" else 0},
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

if ingest_error:
    message = f"Ingest did not return a complete, validated result: {ingest_error}"
    log_table_run(run_id, "pipeline", "pipeline.run_process", "FAILED",
                  batch_from, batch_to, p_mode, error=message)
    raise RuntimeError(message)

gold_result = None
required_gold_tables = [t["table_name"] for t in TABLES if t.get("is_active", True)]
missing_gold_before = [table for table in required_gold_tables if not table_exists(table)]
upstream_tables = sorted({source_table(s) for s in SOURCES if table_exists(source_table(s))})
current_versions = source_versions(upstream_tables)
previous_gold = None
if table_exists("meta.table_run"):
    rows = (spark.table("meta.table_run").where((F.col("layer") == "gld") &
             (F.col("table_name") == "gld.gld_f_sales_line") & (F.col("event") == "SUCCEEDED"))
            .orderBy(F.col("event_at_utc").desc()).limit(1).collect())
    previous_gold = rows[0]["source_versions"] if rows else None
gold_changed = (p_mode == "rerun" or p_full_reload or p_from_layer == "gld" or
                previous_gold is None or bool(missing_gold_before) or
                any(int(current_versions.get(k, -1)) != int((previous_gold or {}).get(k, -2))
                    for k in current_versions))

if gold_changed and p_from_layer in ("ingest", "slv", "gld"):
    log_table_run(run_id, "gld", "pipeline.gold", "STARTED", batch_from, batch_to, p_mode)
    try:
        # Deterministic rebuild from Silver current + append-only snapshot history.
        run_audited_notebook("nb_gld_dims", 7200, {
            "p_run_id": run_id, "p_entities": p_entities, "p_full_reload": p_full_reload},
            "gld", "pipeline.gold_dimensions", batch_from, batch_to, p_mode)

        order_tables = ["slv.slv_wholesale_orders", "slv.slv_retail_orders"]
        available_order_tables = [table for table in order_tables if table_exists(table)]
        requested_orders = (p_entities == "ALL" or any(
            entity.endswith(".orders") for entity in requested_entities))
        if not available_order_tables:
            if p_entities == "ALL" or requested_orders:
                raise RuntimeError(
                    "Gold fact dependency is missing: neither "
                    "slv.slv_wholesale_orders nor slv.slv_retail_orders exists. "
                    "Load an orders delivery into Silver before running Gold fact.")
            gold_result = {"fact_changed": False,
                           "reason": "No Silver orders table; fact skipped for this partial entity scope"}
            log_table_run(run_id, "gld", "gld.gld_f_sales_line", "SKIPPED",
                          batch_from, batch_to, p_mode,
                          error="No orders input selected/present for this partial entity run")
        else:
            gold_result = run_audited_notebook("nb_gld_fact", 7200, {
                "p_run_id": run_id, "p_batch_from": batch_from or "", "p_batch_to": batch_to or "",
                "p_mode": p_mode, "p_full_reload": p_full_reload},
                "gld", "gld.gld_f_sales_line", batch_from, batch_to, p_mode,
                metrics_from_result=lambda result: {"rows_in": result.get("fact_rows", 0),
                                                    "rows_quarantined": result.get("quarantined", 0)},
                source_versions=current_versions)
        if p_entities == "ALL":
            missing_gold_after = [table for table in required_gold_tables if not table_exists(table)]
            if missing_gold_after:
                message = ("Gold build finished without creating required tables: "
                           + ", ".join(missing_gold_after))
                raise RuntimeError(message)
        log_table_run(run_id, "gld", "pipeline.gold", "SUCCEEDED", batch_from, batch_to,
                      p_mode, source_versions=current_versions)
    except Exception as exc:
        log_table_run(run_id, "gld", "pipeline.gold", "FAILED", batch_from, batch_to,
                      p_mode, source_versions=current_versions, error=str(exc))
        parent_failed = (spark.table("meta.table_run")
                         .where((F.col("run_id") == run_id) &
                                (F.col("table_name") == "pipeline.run_process") &
                                (F.col("event") == "FAILED"))
                         .limit(1).count() > 0)
        if not parent_failed:
            log_table_run(run_id, "pipeline", "pipeline.run_process", "FAILED",
                          batch_from, batch_to, p_mode, error=f"Gold stage failed: {exc}")
        raise
else:
    log_table_run(run_id, "gld", "pipeline.gold", "SKIPPED", batch_from, batch_to, p_mode,
                  source_versions=current_versions,
                  error="No upstream Silver version change and no explicit Gold rerun/full reload")

full_scope = p_entities == "ALL"
dq_layer = "all" if full_scope else "slv"
dq_result = run_audited_notebook("nb_dq", 3600, {
    "p_run_id": run_id, "p_batch_id": batch_from or "", "p_layer": dq_layer,
    "p_entities": p_entities},
    "dq", "pipeline.dq" if full_scope else "pipeline.dq.partial_silver",
    batch_from, batch_to, p_mode,
    metrics_from_result=lambda result: {"rows_in": len(result.get("checks", [])),
                                        "rows_quarantined": result.get("quarantine_rows", 0)})
if full_scope:
    recon_result = run_audited_notebook("nb_ops_recon", 3600, {
        "p_run_id": run_id, "p_batch_id": batch_from or ""},
        "recon", "pipeline.reconciliation", batch_from, batch_to, p_mode,
        metrics_from_result=lambda result: {"rows_in": len(result.get("checks", []))})
else:
    recon_result = None
    log_table_run(run_id, "recon", "pipeline.reconciliation", "SKIPPED",
                  batch_from, batch_to, p_mode,
                  error="Partial entity run: full Gold reconciliation is only run for p_entities=ALL")

# Only the master pipeline refreshes the semantic model, after this notebook succeeds.
log_table_run(run_id, "pipeline", "pipeline.run_process",
              "SUCCEEDED" if full_scope else "PARTIAL",
              batch_from, batch_to, p_mode,
              error=None if full_scope else
              "Scoped source run: Silver DQ completed; full Gold DQ/reconciliation not run")
notebookutils.notebook.exit(json.dumps({"run_id": run_id, "silver": loaded,
                                        "gold": gold_result, "dq": dq_result,
                                        "recon": recon_result, "gold_changed": gold_changed,
                                        "scope": "full" if full_scope else "partial"}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

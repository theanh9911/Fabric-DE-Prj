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

# # nb_plan
# Preflight, ghi `pipeline STARTED`, lập kế hoạch ingest (chỉ liệt kê, không đọc nội dung nguồn).
# Spec: docs/04-kien-truc.md §3.2–3.4, §9.1, §11.2. Kết quả (exit value) là đầu vào của `for_each_source` và
# `for_each_master`.

# PARAMETERS CELL ********************

p_run_id = ""
p_load_date = ""
p_load_date_to = ""
p_mode = "normal"
p_scope = "ALL"
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

%run nb_contracts

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

%run nb_std

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

NB_NAME = "nb_plan"
import json

RUN_ID = p_run_id or new_id()
run_start = utc_now()


def plan():
    params = preflight(p_mode, p_load_date, p_load_date_to, p_scope, p_full_reload)
    for t in ("meta.run_log", "meta.landed_file", "meta.v_watermark"):
        require_table(t)
    log_run(RUN_ID, "pipeline", "pipeline", "STARTED", run_mode=params["mode"],
            batch_from=params["batch_id"] or None,
            batch_to=(to_batch_id(params["load_date_to"]) if params["load_date_to"] else params["batch_id"] or None),
            params={"p_load_date": p_load_date, "p_load_date_to": p_load_date_to, "p_mode": p_mode,
                    "p_scope": p_scope, "p_full_reload": params["full_reload"],
                    "p_source_workspace": p_source_workspace, "p_erp_item": p_erp_item,
                    "p_drop_lakehouse": p_drop_lakehouse})
    ids = resolve_source_ids(p_source_workspace, p_erp_item, p_drop_lakehouse)
    ids.update(platform_ids())

    sources, errors = [], []
    if params["mode"] == "normal":
        wanted = [s for s in params["scope"] if s in CONTRACTS]
        contracts = [c for c in active_contracts() if not wanted or c["id"] in wanted]
        watermarks = {(r["source_system"], r["entity"]): r["watermark"]
                      for r in spark.table("meta.v_watermark").collect()}
        landed = {(r["source_system"], r["entity"], r["file_name"], int(r["file_size"]), int(r["sec"]))
                  for r in spark.sql("SELECT source_system, entity, file_name, file_size, "
                                     "unix_timestamp(modify_time_utc) AS sec FROM meta.landed_file").collect()}
        listings = {}
        for c in contracts:
            if c["load_strategy"].startswith("file_"):
                try:
                    listings[c["id"]] = list_drop_zone(ids, c)
                except Exception as exc:  # noqa: BLE001 — một nguồn lỗi không chặn lập kế hoạch nguồn khác
                    errors.append({"source_id": c["id"], "error": f"{type(exc).__name__}: {str(exc)[:400]}"})
        batch_id = params["batch_id"]
        result = plan_ingest([c for c in contracts if c["id"] not in {e["source_id"] for e in errors}],
                             params["load_date"], watermarks, landed, listings, run_start, batch_id)
        sources = result["sources"]
        errors += result["errors"]
        for nd in result["no_data"]:
            log_run(RUN_ID, f"ingest.{nd['source_id']}", "ingest", "NO_DATA", reason=nd["reason"],
                    batch_from=batch_id, batch_to=batch_id)
        for ss in result["superseded"]:
            ss.setdefault("batch_id", batch_id)
        record_landed_files(result["superseded"], RUN_ID)
    for e in errors:
        log_run(RUN_ID, f"ingest.{e['source_id']}", "ingest", "FAILED", error_message=e["error"])
    if errors:
        raise RuntimeError("lập kế hoạch ingest lỗi: " + "; ".join(f"{e['source_id']}: {e['error']}" for e in errors))
    return {"run_id": RUN_ID, "batch_id": params["batch_id"], "load_date": params["load_date"],
            "load_date_to": params["load_date_to"], "run_start_utc": iso_utc(run_start),
            "check_scope": params["check_scope"], "ids": ids, "sources": sources,
            "master_entities": master_entities()}


try:
    PLAN = plan()
except Exception as exc:  # noqa: BLE001
    try:
        log_run(RUN_ID, "pipeline", "pipeline", "FAILED", error_message=f"nb_plan: {exc}")
    except Exception:  # noqa: BLE001 — bảng meta có thể chưa tồn tại; lỗi gốc quan trọng hơn
        pass
    raise
print(json.dumps({k: v for k, v in PLAN.items() if k != "sources"}, indent=2))
print(f"{len(PLAN['sources'])} source: " + ", ".join(s["source_id"] for s in PLAN["sources"]))
notebookutils.notebook.exit(json.dumps(PLAN))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

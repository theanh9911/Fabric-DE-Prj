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

# # nb_dq
# Chạy check nền tảng (`nb_config.CHECKS`) và check contract của các delivery trong lượt; ghi `meta.check_result`.
# Có check critical FAIL ⇒ ghi hết rồi raise (lượt FAILED). Spec: docs/04-kien-truc.md §10.9, §13.

# PARAMETERS CELL ********************

p_run_id = ""
p_batch_id = ""
p_check_scope = "daily"

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

NB_NAME = "nb_dq"
RUN_ID, BATCH_ID, SCOPE = p_run_id, p_batch_id, (p_check_scope or "daily")
INACTIVE = {o["table"] for o in SILVER + GOLD if not o["is_active"]}


def run_all():
    results = []  # (check_id, status, severity)
    for spec in CHECKS:
        if spec["check_id"].startswith("DQ-OPS-01") or spec["object"] in INACTIVE:
            continue  # DQ-OPS-01 do nb_sla_watch chạy; object chưa bật thì chưa có gì để kiểm
        try:
            st = run_check(spec, RUN_ID, BATCH_ID, SCOPE)
        except Exception as exc:  # noqa: BLE001 — check hỏng là lỗi vận hành, không được nuốt
            log_check(RUN_ID, BATCH_ID, spec["check_id"], "platform", spec["type"], spec["object"], spec["scope"],
                      0, None, 0, spec["severity"], message=f"check lỗi: {type(exc).__name__}: {str(exc)[:300]}",
                      status="FAIL")
            st = "FAIL"
        if st is not None:
            results.append((spec["check_id"], st, spec["severity"]))
    deliveries = deliveries_of_run(RUN_ID, ("LANDED", "LOADED"))
    for check_id, delivery_id, st in run_contract_checks(deliveries, RUN_ID):
        results.append((f"{check_id}@{delivery_id[:8]}", st, "critical" if st == "FAIL" else "warning"))
    return results


try:
    RESULTS = run_all()
except Exception as exc:  # noqa: BLE001
    log_run(RUN_ID, "dq", "dq", "FAILED", error_message=f"nb_dq: {type(exc).__name__}: {str(exc)[:1500]}")
    raise
FAILED = [r[0] for r in RESULTS if r[1] == "FAIL"]
WARNED = [r[0] for r in RESULTS if r[1] == "WARN"]
log_run(RUN_ID, "dq", "dq", "FAILED" if FAILED else "SUCCEEDED", rows_in=len(RESULTS), rows_flagged=len(WARNED),
        error_message=("critical FAIL: " + ", ".join(FAILED)) if FAILED else None)
if FAILED:
    raise RuntimeError("check critical FAIL: " + ", ".join(FAILED))
exit_worker({"contract": 1, "status": "SUCCEEDED", "checks": len(RESULTS), "warnings": len(WARNED)})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

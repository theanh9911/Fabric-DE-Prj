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

# # nb_close
# Đóng lượt: ghi `meta.run_log` (`step = pipeline`, `event = p_status`). Khi `FAILED`, kèm các lỗi mà notebook của
# lượt đã ghi vào `run_log`. Spec: docs/04-kien-truc.md §10.1, §10.9.

# PARAMETERS CELL ********************

p_run_id = ""
p_status = "SUCCEEDED"
p_error = ""

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

NB_NAME = "nb_close"
import json

if p_status not in ("SUCCEEDED", "FAILED"):
    raise ValueError(f"p_status không hợp lệ: {p_status!r}")
msg = p_error or None
if p_status == "FAILED":
    rows = spark.sql("SELECT step, error_message FROM meta.run_log "
                     f"WHERE run_id = '{p_run_id}' AND event = 'FAILED' AND error_message IS NOT NULL "
                     "ORDER BY event_at_utc").collect()
    detail = "; ".join(f"{r['step']}: {r['error_message']}" for r in rows)
    msg = " | ".join(x for x in (p_error, detail) if x) or "pipeline FAILED (xem Monitor)"
log_run(p_run_id, "pipeline", "pipeline", p_status, error_message=msg)
exit_worker({"status": p_status, "run_id": p_run_id})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

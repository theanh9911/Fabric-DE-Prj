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

# # nb_sla_watch
# DQ-OPS-01: nguồn có `deliveryCadence = daily` mà chưa có delivery `LOADED` cho batch `p_load_date` sau
# `availableByLocalTime` của ngày kế tiếp (giờ VN). Spec: docs/04-kien-truc.md §10.9, §13.
# Kết quả: `{"late_sources": [{"source_id", "owner", "support_channel"}]}`.

# PARAMETERS CELL ********************

p_run_id = ""
p_load_date = ""

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

NB_NAME = "nb_sla_watch"
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

RUN_ID = p_run_id or new_id()
LOAD_DATE = p_load_date or default_load_date()
BATCH_ID = to_batch_id(LOAD_DATE)
now_local = datetime.now(ZoneInfo(SETTINGS["business_timezone"])).replace(tzinfo=None)

late = []
for c in active_contracts():
    if c["delivery_cadence"] != "daily":
        continue
    hh, mm = (int(x) for x in c["available_by_local_time"].split(":"))
    deadline = datetime.strptime(LOAD_DATE, "%Y-%m-%d") + timedelta(days=1, hours=hh, minutes=mm)
    if now_local < deadline:
        continue
    n = spark.sql("SELECT count(*) AS n FROM meta.v_delivery_state WHERE status = 'LOADED' "
                  f"AND source_system = '{c['source_system']}' AND entity = '{c['entity']}' "
                  f"AND batch_id = '{BATCH_ID}'").collect()[0]["n"]
    log_check(RUN_ID, BATCH_ID, "DQ-OPS-01", "platform", "freshness", c["id"], LOAD_DATE, 0, 0 if n else 1, 0,
              "warning", message=f"{c['id']}: {'đã có' if n else 'chưa có'} delivery LOADED cho batch {BATCH_ID}"
                                 f" (hạn {deadline:%Y-%m-%d %H:%M})")
    if not n:
        late.append({"source_id": c["id"], "owner": c["owner"], "support_channel": c["support_channel"]})
log_run(RUN_ID, "sla_watch", "ops", "SUCCEEDED", reason=f"late={len(late)}", batch_from=BATCH_ID, batch_to=BATCH_ID)
notebookutils.notebook.exit(json.dumps({"late_sources": late}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

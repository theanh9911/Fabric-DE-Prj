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

# # nb_maintenance
# `OPTIMIZE` mọi bảng `meta`, `slv`, `gld`; `VACUUM RETAIN 720 HOURS` bảng `slv`, `gld` (30 ngày > `cdf_max_age_days`);
# xoá thư mục staging cũ hơn 1 ngày. Không đụng Landing (L2). Spec: docs/04-kien-truc.md §15.

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

NB_NAME = "nb_maintenance"
import json
import time

RUN_ID = new_id()
RETAIN_HOURS = 720
optimized, vacuumed, removed = [], [], []


def tables_of(schema):
    out = []
    for r in spark.sql(f"SHOW TABLES IN {schema}").collect():
        name = f"{schema}.{r['tableName']}"
        if spark.catalog.getTable(name).tableType.upper() != "VIEW":
            out.append(name)
    return out


try:
    for schema in ("meta", "slv", "gld"):
        for t in tables_of(schema):
            spark.sql(f"OPTIMIZE {safe_table_name(t)}")
            optimized.append(t)
            if schema in ("slv", "gld"):
                spark.sql(f"VACUUM {safe_table_name(t)} RETAIN {RETAIN_HOURS} HOURS")
                vacuumed.append(t)
    if notebookutils.fs.exists("Files/_staging"):
        cutoff_ms = int((time.time() - 24 * 3600) * 1000)
        for d in notebookutils.fs.ls("Files/_staging"):
            if d.modifyTime is not None and d.modifyTime < cutoff_ms:
                notebookutils.fs.rm(d.path, True)
                removed.append(d.name)
except Exception as exc:  # noqa: BLE001
    log_run(RUN_ID, "maintenance", "maint", "FAILED", error_message=f"{type(exc).__name__}: {str(exc)[:1500]}")
    raise
log_run(RUN_ID, "maintenance", "maint", "SUCCEEDED", rows_in=len(optimized),
        reason=f"optimized={len(optimized)} vacuumed={len(vacuumed)} staging_removed={len(removed)}")
notebookutils.notebook.exit(json.dumps({"optimized": optimized, "vacuumed": vacuumed, "staging_removed": removed}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

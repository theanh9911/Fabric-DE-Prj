# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

p_load_date = ""

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

from datetime import datetime, timedelta, timezone
import json

if not p_load_date:
    p_load_date = ((datetime.now(timezone.utc) + timedelta(hours=7)).date() - timedelta(days=1)).isoformat()
batch_id = p_load_date.replace("-", "")
run_id = str(uuid.uuid4())
missing = []
for source in SOURCES:
    if source.get("expected") != "daily" or not source.get("is_active", True):
        continue
    present = (spark.table("meta.ingestion_batch")
               .where((F.col("source_system") == source["source_system"]) &
                      (F.col("entity") == source["entity"]) &
                      (F.col("batch_id") == batch_id) & (F.col("status") == "LOADED"))
               .limit(1).count() > 0)
    if not present:
        missing.append(f"{source['source_system']}.{source['entity']}")
        log_check(run_id, batch_id, f"sla.{source['source_system']}.{source['entity']}",
                  "sla", f"{source['source_system']}.{source['entity']}", batch_id,
                  1, 0, 0, "warning")

notebookutils.notebook.exit(json.dumps({"run_id": run_id, "batch_id": batch_id,
                                        "missing_daily_sources": missing}))

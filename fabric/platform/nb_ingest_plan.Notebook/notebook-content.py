# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

p_load_date = ""
p_window_start = ""
p_window_end = ""
p_entities = "ALL"
p_mode = "normal"
p_from_layer = "ingest"

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

from datetime import date, datetime, timedelta, timezone
import json

if not p_load_date:
    p_load_date = ((datetime.now(timezone.utc) + timedelta(hours=7)).date() - timedelta(days=1)).isoformat()
batch_id = p_load_date.replace("-", "")
run_id = str(uuid.uuid4())
wm = {}
if table_exists("meta.v_watermark"):
    for row in spark.table("meta.v_watermark").collect():
        wm[(row["source_system"], row["entity"])] = row.asDict()

selected = [s for s in SOURCES if s.get("is_active", True)]
if p_mode == "rerun" or p_from_layer != "ingest":
    selected = []
if p_entities != "ALL":
    requested = {x.strip() for x in p_entities.split(",")}
    selected = [s for s in selected if f"{s['source_system']}.{s['entity']}" in requested]

plan = []
for source in sorted(selected, key=lambda x: (x["load_order"], x["source_system"], x["entity"])):
    prior = wm.get((source["source_system"], source["entity"]), {})
    watermark = prior.get("watermark_value")
    watermark_dt = None
    if watermark:
        try:
            watermark_dt = datetime.fromisoformat(str(watermark).replace("Z", "+00:00"))
            if watermark_dt.tzinfo is not None:
                watermark_dt = watermark_dt.astimezone(timezone.utc).replace(tzinfo=None)
        except Exception:
            watermark_dt = None
    lookback = int(source.get("lookback_minutes", 0))
    window_start = p_window_start or (watermark_dt - timedelta(minutes=lookback)).isoformat(sep=" ") if watermark_dt else (p_window_start or watermark)
    item = {**source, "batch_id": batch_id, "run_id": run_id,
            "delivery_id": run_id, "window_start": window_start,
            "window_end": p_window_end, "run_start_utc": datetime.now(timezone.utc).isoformat(),
            "last_checksum": prior.get("source_checksum"),
            "force_copy": datetime.now(timezone.utc).weekday() == 6 or bool(p_window_start)}
    if source["load_strategy"] == "db_incremental" and not item["window_start"]:
        item["window_start"] = "1900-01-01 00:00:00"
    if source.get("source_clock") == "virtual" and not item["window_end"]:
        item["window_end"] = (date.fromisoformat(p_load_date) + timedelta(days=1)).isoformat()
    if source["load_strategy"] == "file_new_or_changed" and not item["window_end"]:
        item["window_end"] = (datetime.now(timezone.utc) - timedelta(minutes=int(source.get("settle_minutes", 0)))).isoformat()
    plan.append(item)

notebookutils.notebook.exit(json.dumps({"run_id": run_id, "batch_id": batch_id,
                                        "load_date": p_load_date, "sources": plan}, default=str))

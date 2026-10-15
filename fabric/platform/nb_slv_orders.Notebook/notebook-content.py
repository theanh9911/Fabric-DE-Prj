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

# # nb_slv_orders
# Landing → `slv.orders_event`. Rules: R-STD-01…08, R-ORD-01, R-ORD-02, R-ORD-03, R-ORD-05.
# Spec: docs/04-kien-truc.md §5.2, §7, §9.2. Delivery thiếu cột bắt buộc ⇒ `FAILED`, delivery khác vẫn nạp;
# còn delivery `FAILED` chưa được thay thế ⇒ raise để chặn Gold.

# PARAMETERS CELL ********************

p_run_id = ""
p_load_date = ""
p_load_date_to = ""
p_mode = "normal"
p_scope = "ALL"
p_full_reload = False

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

NB_NAME = "nb_slv_orders"
from functools import reduce

TARGET = "slv.orders_event"
SPEC = get_silver(TARGET)
ENTITY = SPEC["entity"]
RUN_ID = p_run_id or new_id()
MODE = p_mode or "normal"
PAIRS = [(c["source_system"], c["entity"]) for c in contracts_for_silver(TARGET)]
DROP_COLS = ["_snapshot_date", "_reject_reason"]


def build(decision):
    full = decision["exec_mode"] == "full"
    deliveries = select_deliveries(TARGET, mode=MODE, load_from=p_load_date or None, load_to=p_load_date_to or None,
                                   full=full)
    ok = []
    for d in deliveries:
        added, missing, missing_req = delivery_schema(d)
        d["_added"], d["_missing"] = added, missing
        if missing_req:
            msg = f"schema_missing_required:{','.join(missing_req)}"
            if not (d.get("status") == "FAILED" and d.get("error_message") == msg):
                record_delivery(d, "FAILED", RUN_ID, schema_added=added, schema_missing=missing,
                                error_message=msg, logic_version=SPEC["logic_version"])
        else:
            ok.append(d)
    if not ok:
        return {"skip_state": True, "rows_in": 0}
    conf = (reduce(lambda a, b: a.unionByName(b), [conform_delivery(d, SPEC) for d in ok])
            .withColumn("_run_id", F.lit(RUN_ID)).withColumn("_logic_version", F.lit(SPEC["logic_version"]))
            .withColumn("_ingested_at_utc", F.current_timestamp()).persist())
    rej = F.col("_reject_reason").isNotNull()
    stats = {r["_delivery_id"]: r for r in conf.groupBy("_delivery_id").agg(
        F.count(F.lit(1)).alias("n"), F.sum(F.when(rej, 1).otherwise(0)).alias("rejected"),
        F.sum(F.when(~rej & (F.size(F.coalesce("_dq_flags", F.array().cast("array<string>"))) > 0), 1)
              .otherwise(0)).alias("flagged")).collect()}
    n_reject, n_flag = quarantine_rows(conf, ENTITY, RUN_ID)
    w = Window.partitionBy("_event_key").orderBy(F.col("_delivered_at_utc").desc(), F.col("_row_no").desc())
    acc = (conf.where(~rej).withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
           .drop("_rn", *DROP_COLS))
    m = overwrite_table(TARGET, acc) if full else merge_events(TARGET, acc)
    resolve_quarantine(ENTITY, acc.select("_bk", "_delivery_id", "_delivered_at_utc"), RUN_ID)
    for d in ok:
        s = stats.get(d["delivery_id"])
        n, r, fl = (int(s["n"]), int(s["rejected"]), int(s["flagged"])) if s else (0, 0, 0)
        record_delivery(d, "LOADED", RUN_ID, schema_added=d["_added"], schema_missing=d["_missing"],
                        rows_read=n, rows_accepted=n - r, rows_rejected=r, rows_flagged=fl,
                        logic_version=SPEC["logic_version"])
    conf.unpersist()
    return {"rows_in": sum(int(s["n"]) for s in stats.values()), "rows_inserted": m["rows_inserted"],
            "rows_updated": m["rows_updated"], "rows_deleted": m["rows_deleted"], "rows_rejected": n_reject,
            "rows_flagged": n_flag}


RESULT = run_node(TARGET, build, RUN_ID, parse_bool(p_full_reload), p_scope, MODE, p_load_date or None,
                  p_load_date_to or None)
cond = " OR ".join(f"(source_system = '{s}' AND entity = '{e}')" for s, e in PAIRS)
BLOCKING = spark.sql(f"SELECT delivery_id, source_system, entity, error_message FROM meta.v_pending_delivery "
                     f"WHERE status = 'FAILED' AND ({cond})").collect()
if BLOCKING:
    raise RuntimeError("delivery FAILED chặn Gold: " + "; ".join(
        f"{r['source_system']}.{r['entity']} {r['delivery_id']}: {r['error_message']}" for r in BLOCKING))
exit_worker(RESULT)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

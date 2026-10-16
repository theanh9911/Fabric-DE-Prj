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

# # nb_slv_master
# Landing → `slv.<entity>_history` cho một entity master (`p_entity`). Rules: R-STD-01…08, R-MST-01, R-MST-02.
# Spec: docs/04-kien-truc.md §5.4–5.5, §9.3. Snapshot đến sau (hoặc cùng ngày) snapshot đã có ⇒ dựng lại đầy đủ.

# PARAMETERS CELL ********************

p_run_id = ""
p_entity = ""
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

NB_NAME = "nb_slv_master"
from functools import reduce

SPEC = next((s for s in SILVER if s["entity"] == p_entity and s["kind"] == "snapshot"), None)
if SPEC is None:
    raise ValueError(f"p_entity không hợp lệ: {p_entity!r}")
TARGET = SPEC["table"]
RUN_ID = p_run_id or new_id()
MODE = p_mode or "normal"
KEYS = SPEC["business_keys"]
PAIRS = [(c["source_system"], c["entity"]) for c in contracts_for_silver(TARGET)]
HIST_COLS = [c for c in spark.table(TARGET).columns if c != "_change_kind"]


def pick_deliveries(decision):
    """Delivery cần xử lý; chuyển full nếu có snapshot không mới hơn snapshot đã có (04 §9.3 bước 3)."""
    full = decision["exec_mode"] == "full"
    ds = select_deliveries(TARGET, mode=MODE, load_from=p_load_date or None, load_to=p_load_date_to or None,
                           full=full)
    if not full and ds:
        have = {r["source_system"]: str(r["d"]) for r in
                spark.sql(f"SELECT source_system, max(_snapshot_date) AS d FROM {TARGET} GROUP BY source_system"
                          ).collect()}
        if any(d["snapshot_date_str"] and d["snapshot_date_str"] <= have.get(d["source_system"], "0000-00-00")
               for d in ds):
            return select_deliveries(TARGET, full=True), "full", "out_of_order_snapshot"
    return ds, decision["exec_mode"], None


def build(decision):
    deliveries, exec_mode, reason = pick_deliveries(decision)
    full = exec_mode == "full"
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
    # cùng nguồn, cùng snapshot_date: chỉ delivery mới nhất có hiệu lực (file gửi lại)
    newest = {}
    for d in ok:
        k = (d["source_system"], d["snapshot_date_str"])
        if k not in newest or d["delivered_at_str"] > newest[k]["delivered_at_str"]:
            newest[k] = d
    effective = list(newest.values())
    conf = (reduce(lambda a, b: a.unionByName(b), [conform_master(d, SPEC) for d in effective])
            .withColumn("_run_id", F.lit(RUN_ID)).withColumn("_logic_version", F.lit(SPEC["logic_version"]))
            .withColumn("_ingested_at_utc", F.current_timestamp()).persist())
    rej = F.col("_reject_reason").isNotNull()
    stats = {r["_delivery_id"]: r for r in conf.groupBy("_delivery_id").agg(
        F.count(F.lit(1)).alias("n"), F.sum(F.when(rej, 1).otherwise(0)).alias("rejected"),
        F.sum(F.when(~rej & (F.size(F.coalesce("_dq_flags", F.array().cast("array<string>"))) > 0), 1)
              .otherwise(0)).alias("flagged")).collect()}
    n_reject, n_flag = quarantine_rows(conf, SPEC["entity"], RUN_ID)
    snaps = (conf.where(~rej & ~F.col("_is_dup")).withColumn("is_deleted", F.lit(False)).select(*HIST_COLS))
    if full:
        previous = spark.table(TARGET).limit(0)
    else:
        previous = spark.table(f"slv.v_{SPEC['entity']}_current")
    changes = snapshot_changes(snaps, previous, KEYS)
    if full:
        m = overwrite_table(TARGET, changes)
    else:
        m = merge_insert_only(TARGET, changes, ["source_system"] + KEYS + ["_snapshot_date"])
    for d in ok:
        s = stats.get(d["delivery_id"])
        n, r, fl = (int(s["n"]), int(s["rejected"]), int(s["flagged"])) if s else (0, 0, 0)
        record_delivery(d, "LOADED", RUN_ID, schema_added=d["_added"], schema_missing=d["_missing"],
                        rows_read=n, rows_accepted=max(n - r, 0), rows_rejected=r, rows_flagged=fl,
                        logic_version=SPEC["logic_version"])
    conf.unpersist()
    return {"exec_mode": exec_mode, "reason": reason or decision["reason"],
            "rows_in": sum(int(s["n"]) for s in stats.values()), "rows_inserted": m["rows_inserted"],
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

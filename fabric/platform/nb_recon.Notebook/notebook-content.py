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

# # nb_recon
# Đối soát ranh giới (docs/04-kien-truc.md §14): hai số tính độc lập. RC-01 nguồn ↔ Landing; RC-02, RC-03
# Landing ↔ Silver orders; RC-04 Landing ↔ Silver master; RC-05 Silver ↔ Gold. Chỉ chạy phần của object đã bật.
# Có critical FAIL ⇒ ghi hết rồi raise.

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

NB_NAME = "nb_recon"
RUN_ID, BATCH_ID = p_run_id, p_batch_id
ACTIVE = {o["table"] for o in SILVER + GOLD if o["is_active"]}
RESULTS = []
# Bảng vừa được dựng lại hoàn toàn: đối soát lại mọi delivery, không tin kết quả PASS của lần trước.
REBUILT = rebuilt_since_last_recon()


def rc(check_id, delivery_or_obj, a, b, severity, message, scope_value=None):
    st = log_check(RUN_ID, BATCH_ID, check_id, "recon", "reconcile", delivery_or_obj, scope_value, a, b, 0,
                   severity, message=message)
    RESULTS.append((f"{check_id}:{delivery_or_obj}", st, severity))


def rc01():
    """Nguồn ↔ Landing (DB): rowsRead của Copy so với số dòng đếm lại ở Landing."""
    for d in deliveries_to_reconcile("RC-01", ("LANDED",), table="meta.delivery_log"):
        if not str(d["load_strategy"]).startswith("db_"):
            continue
        n = spark.read.parquet(d["landing_path"]).count()
        sev = "critical" if d["load_strategy"] == "db_incremental" else "warning"
        rc("RC-01", f"{d['source_system']}.{d['entity']}", d["source_count"], n, sev,
           "source_count (Copy rowsRead) so với số dòng Landing", d["delivery_id"])


def rc02_rc03():
    spec = get_silver("slv.orders_event")
    for d in deliveries_to_reconcile("RC-02", ("LOADED",), include_checked=REBUILT):
        if f"{d['source_system']}.{d['entity']}" not in {c["id"] for c in contracts_for_silver("slv.orders_event")}:
            continue
        conf = conform_delivery(d, spec).persist()
        accepted = conf.where("_reject_reason IS NULL").select("_event_key").distinct()
        missing = accepted.join(spark.table("slv.orders_event").select("_event_key").distinct(), "_event_key",
                                "left_anti").count()
        name = f"{d['source_system']}.{d['entity']}"
        rc("RC-02", name, 0, missing, "critical", "khoá sự kiện hợp lệ của Landing thiếu ở slv.orders_event",
           d["delivery_id"])
        n_reject = quarantine_frame(conf, "orders", RUN_ID).where("action = 'REJECT'").count()
        n_q = (spark.table("slv.quarantine")
               .where((F.col("delivery_id") == d["delivery_id"]) & (F.col("action") == "REJECT")).count())
        rc("RC-03", name, n_reject, n_q, "critical", "dòng REJECT khi conform lại so với slv.quarantine",
           d["delivery_id"])
        conf.unpersist()


def rc04():
    latest = {}
    for d in deliveries_to_reconcile("RC-04", ("LOADED",), latest_per_key=True, include_checked=REBUILT):
        tbl = next((c["silver_table"] for c in active_contracts()
                    if c["source_system"] == d["source_system"] and c["entity"] == d["entity"]), None)
        if tbl is None or get_silver(tbl)["kind"] != "snapshot" or tbl not in ACTIVE:
            continue
        key = (d["source_system"], d["entity"])
        if key not in latest or str(d["snapshot_date_str"]) > str(latest[key]["snapshot_date_str"]):
            latest[key] = dict(d, silver_table=tbl)
    for (ss, ent), d in latest.items():
        spec = get_silver(d["silver_table"])
        snap = (conform_master(d, spec).where("_reject_reason IS NULL AND NOT _is_dup")
                .select(F.col("_bk"), F.col("_record_hash").alias("snap_hash")))
        cur = (spark.table(f"slv.v_{ent}_current").where(F.col("source_system") == ss)
               .select("_bk", F.col("_record_hash").alias("cur_hash"), "is_deleted"))
        j = snap.join(cur, "_bk", "full_outer")
        bad = j.where("(snap_hash IS NOT NULL AND (cur_hash IS NULL OR is_deleted OR cur_hash <> snap_hash)) "
                      "OR (snap_hash IS NULL AND cur_hash IS NOT NULL AND NOT is_deleted)").count()
        rc("RC-04", f"{ss}.{ent}", 0, bad, "critical", "khoá lệch giữa snapshot mới nhất và v_<entity>_current",
           d["delivery_id"])


def rc05():
    a = spark.sql("SELECT count(*) AS n, coalesce(sum(quantity * price), 0) AS amt FROM slv.v_orders_current"
                  ).collect()[0]
    b = spark.sql("SELECT count(*) AS n, coalesce(sum(quantity * price), 0) AS amt FROM gld.fct_order_line"
                  ).collect()[0]
    rc("RC-05", "gld.fct_order_line", a["n"], b["n"], "critical", "số dòng: slv.v_orders_current so với fact")
    rc("RC-05", "gld.fct_order_line.amount", a["amt"], b["amt"], "critical",
       "sum(quantity * price): slv.v_orders_current so với fact")


def run_all():
    rc01()
    if "slv.orders_event" in ACTIVE:
        rc02_rc03()
    rc04()
    if "gld.fct_order_line" in ACTIVE:
        rc05()


try:
    run_all()
except Exception as exc:  # noqa: BLE001
    log_run(RUN_ID, "recon", "recon", "FAILED", error_message=f"nb_recon: {type(exc).__name__}: {str(exc)[:1500]}")
    raise
FAILED = [r[0] for r in RESULTS if r[1] == "FAIL"]
WARNED = [r[0] for r in RESULTS if r[1] == "WARN"]
log_run(RUN_ID, "recon", "recon", "FAILED" if FAILED else "SUCCEEDED", rows_in=len(RESULTS), rows_flagged=len(WARNED),
        error_message=("critical FAIL: " + ", ".join(FAILED)) if FAILED else None)
if FAILED:
    raise RuntimeError("đối soát FAIL: " + ", ".join(FAILED))
exit_worker({"contract": 1, "status": "SUCCEEDED", "checks": len(RESULTS), "warnings": len(WARNED)})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

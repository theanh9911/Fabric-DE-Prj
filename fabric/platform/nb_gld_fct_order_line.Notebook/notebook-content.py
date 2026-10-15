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

# # nb_gld_fct_order_line
# Rules: R-ORD-04, R-SLM-05. Spec: docs/04-kien-truc.md §6.5, §9.7.

# PARAMETERS CELL ********************

p_run_id = ""
p_full_reload = False
p_scope = "ALL"

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

NB_NAME = "nb_gld_fct_order_line"
TARGET = "gld.fct_order_line"
SPEC = get_gold(TARGET)
GRAIN = ["source_system", "order_no", "product_code"]
STATUSES = sorted(set(ORDER_STATUS.values()), key=lambda s: STATUS_RANK[s])
BUSINESS = (["customer_code", "salesman_code", "customer_sk", "product_sk", "salesman_sk", "order_date_key",
             "current_status", "quantity", "price", "tax_rate"] + [f"first_{s.lower()}_at_utc" for s in STATUSES]
            + ["last_event_at_utc", "status_event_count"])


def touched_keys(iv):
    """R-ORD-04 / 04 §9.7 bước 1 (incremental): khoá có sự kiện mới, và khoá của salesman có phiên bản đổi."""
    r = iv["slv.orders_event"]
    k1 = read_changes("slv.orders_event", r["from"], r["to"]).select(*GRAIN).distinct()
    s = iv["gld.dim_salesman"]
    sm = read_changes("gld.dim_salesman", s["from"], s["to"]).select("source_system", "salesman_code").distinct()
    k2 = (spark.table("slv.orders_event").join(sm, ["source_system", "salesman_code"], "left_semi")
          .select(*GRAIN).distinct())
    k = k1.unionByName(k2).distinct()
    k.limit(1).collect()
    return k


def build(decision):
    full = decision["exec_mode"] == "full"
    reason = decision["reason"]
    events = spark.table("slv.orders_event")
    if not full:
        try:
            events = events.join(touched_keys(decision["input_versions"]), GRAIN, "left_semi")
        except CdfUnavailable:
            full, reason = True, "cdf_unavailable"
            events = spark.table("slv.orders_event")
    cur = latest_event(events)
    ms = events.groupBy(*GRAIN).agg(
        F.max("updated_at_utc").alias("last_event_at_utc"), F.count(F.lit(1)).cast("int").alias("status_event_count"),
        *[F.min(F.when(F.col("order_status") == s, F.col("updated_at_utc"))).alias(f"first_{s.lower()}_at_utc")
          for s in STATUSES])
    f = cur.select(*GRAIN, "customer_code", "salesman_code", "order_date", F.col("order_status").alias("current_status"),
                   "quantity", "price", "tax_rate").join(ms, GRAIN, "inner")
    sm = spark.table("gld.dim_salesman").select(
        "source_system", "salesman_code", "salesman_sk", "valid_from", "valid_to")
    f = (f.alias("f").join(sm.alias("s"),
                           (F.col("f.source_system") == F.col("s.source_system"))
                           & (F.col("f.salesman_code") == F.col("s.salesman_code"))
                           & F.col("f.order_date").between(F.col("s.valid_from"), F.col("s.valid_to")), "left")
         .select("f.*", F.coalesce(F.col("s.salesman_sk"), F.lit(-1).cast("bigint")).alias("salesman_sk")))
    out = (f.withColumn("customer_sk", F.when(F.col("customer_code").isNull(), F.lit(-1).cast("bigint"))
                        .otherwise(F.expr(sk("customer_code"))))
           .withColumn("product_sk", F.expr(sk("product_code")))
           .withColumn("order_date_key", F.expr(date_key("order_date")))
           .drop("order_date"))
    out = out.withColumn("_row_hash", F.expr(record_hash(BUSINESS)))
    out = gold_audit(out, RUN_ID, SPEC["logic_version"]).persist()
    n = out.count()
    m = overwrite_table(TARGET, out) if full else merge_upsert(TARGET, out, GRAIN, "_row_hash")
    out.unpersist()
    return {"exec_mode": "full" if full else "incremental", "reason": reason, "rows_in": n,
            "rows_inserted": m["rows_inserted"], "rows_updated": m["rows_updated"], "rows_deleted": m["rows_deleted"]}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

RUN_ID = p_run_id or new_id()
RESULT = run_node(TARGET, build, RUN_ID, parse_bool(p_full_reload), p_scope)
exit_worker(RESULT)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

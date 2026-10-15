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

# # nb_gld_dim_salesman
# Rules: R-SLM-01…R-SLM-04. Spec: docs/04-kien-truc.md §6.4, §9.6.

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

def reduce_union(frames):
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f, allowMissingColumns=True)
    return out

NB_NAME = "nb_gld_dim_salesman"
TARGET = "gld.dim_salesman"
SPEC = get_gold(TARGET)
PENDING = "Unknown (pending)"
HASH_COLS = ["salesman_name", "division", "position", "manager_code", "manager_name", "valid_from", "valid_to",
             "is_current", "is_deleted", "is_inferred"]

def source_codes(full, iv, code_col):
    """(code, source_system) có trong orders: full ⇒ dòng hiện hành; incremental ⇒ CDF của orders_event."""
    if full:
        ev = latest_event(spark.table("slv.orders_event"))
    else:
        r = iv["slv.orders_event"]
        ev = read_changes("slv.orders_event", r["from"], r["to"]).where(
            "_change_type IN ('insert', 'update_postimage')")
    return ev.where(F.col(code_col).isNotNull()).select(code_col, "source_system").distinct()


def versions():
    """R-SLM-01…03: mốc thay đổi ⇒ phiên bản; manager_name theo phiên bản của quản lý có hiệu lực tại valid_from."""
    return spark.sql("""
        WITH h AS (
          SELECT source_system, salesman_code, salesman_name, division, position,
                 salesmanager_code AS manager_code, is_deleted, _snapshot_date, _delivered_at_utc,
                 sha2(concat_ws(char(31), coalesce(salesman_name, '<NULL>'), coalesce(division, '<NULL>'),
                                coalesce(position, '<NULL>'), coalesce(salesmanager_code, '<NULL>'),
                                CAST(is_deleted AS STRING)), 256) AS _chg
          FROM slv.sales_hierarchy_history),
        c AS (SELECT *, lag(_chg) OVER (PARTITION BY source_system, salesman_code
                                        ORDER BY _snapshot_date, _delivered_at_utc) AS _prev FROM h),
        v AS (SELECT * FROM c WHERE _prev IS NULL OR _prev <> _chg),
        d AS (SELECT *, row_number() OVER w AS _vn, lead(_snapshot_date) OVER w AS _next_from
              FROM v WINDOW w AS (PARTITION BY source_system, salesman_code ORDER BY _snapshot_date, _delivered_at_utc)),
        e AS (SELECT source_system, salesman_code, salesman_name, division, position, manager_code, is_deleted,
                     CASE WHEN _vn = 1 THEN DATE'1900-01-01' ELSE _snapshot_date END AS valid_from,
                     CASE WHEN _next_from IS NULL THEN DATE'9999-12-31' ELSE date_sub(_next_from, 1) END AS valid_to,
                     _next_from IS NULL AS is_current
              FROM d)
        SELECT e.source_system, e.salesman_code, e.salesman_name, e.division, e.position, e.manager_code,
               m.salesman_name AS manager_name, e.valid_from, e.valid_to, e.is_current, e.is_deleted,
               false AS is_inferred
        FROM e LEFT JOIN e m ON m.source_system = e.source_system AND m.salesman_code = e.manager_code
                             AND e.valid_from BETWEEN m.valid_from AND m.valid_to""")


def build(decision):
    full = decision["exec_mode"] == "full"
    reason = decision["reason"]
    iv = decision["input_versions"]
    try:
        codes = source_codes(full, iv, "salesman_code")
        codes.limit(1).collect()
    except CdfUnavailable:
        full, reason = True, "cdf_unavailable"
        codes = source_codes(True, iv, "salesman_code")
    vers = versions()
    existing = spark.table(TARGET)
    known = existing.where("false") if full else existing   # full: dựng lại, không giữ inferred cũ
    keys = ["source_system", "salesman_code"]
    inferred = (codes.join(vers.select(*keys).distinct(), keys, "left_anti")
                .join(known.select(*keys).distinct(), keys, "left_anti")
                .withColumn("salesman_name", F.lit(PENDING)).withColumn("division", F.lit(None).cast("string"))
                .withColumn("position", F.lit(None).cast("string"))
                .withColumn("manager_code", F.lit(None).cast("string"))
                .withColumn("manager_name", F.lit(None).cast("string"))
                .withColumn("valid_from", F.lit("1900-01-01").cast("date"))
                .withColumn("valid_to", F.lit("9999-12-31").cast("date"))
                .withColumn("is_current", F.lit(True)).withColumn("is_deleted", F.lit(False))
                .withColumn("is_inferred", F.lit(True)))
    parts = [vers, inferred]
    if not full:
        keep = (existing.where("is_inferred").where("salesman_sk <> -1")
                .join(vers.select(*keys).distinct(), keys, "left_anti")
                .select(*(keys + ["salesman_name", "division", "position", "manager_code", "manager_name",
                                  "valid_from", "valid_to", "is_current", "is_deleted", "is_inferred"])))
        parts.append(keep)
    unknown = spark.sql("SELECT '#' AS source_system, '#UNKNOWN' AS salesman_code, 'Unknown (pending)' AS salesman_name, "
                        "CAST(NULL AS STRING) AS division, CAST(NULL AS STRING) AS position, "
                        "CAST(NULL AS STRING) AS manager_code, CAST(NULL AS STRING) AS manager_name, "
                        "DATE'1900-01-01' AS valid_from, DATE'9999-12-31' AS valid_to, true AS is_current, "
                        "false AS is_deleted, false AS is_inferred")
    src = reduce_union(parts + [unknown])
    src = (src.withColumn("salesman_sk", F.when(F.col("salesman_code") == "#UNKNOWN", F.lit(-1).cast("bigint"))
                          .otherwise(F.expr(sk("source_system", "salesman_code", "CAST(valid_from AS STRING)"))))
           .withColumn("_scd_hash", F.expr(record_hash(HASH_COLS))))
    m = sync_table(TARGET, gold_audit(src, RUN_ID, SPEC["logic_version"]), ["salesman_sk"], "_scd_hash")
    return {"exec_mode": "full" if full else "incremental", "reason": reason, "rows_in": src.count(),
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

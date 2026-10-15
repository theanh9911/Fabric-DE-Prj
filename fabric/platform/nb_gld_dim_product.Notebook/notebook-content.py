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

# # nb_gld_dim_product
# Rules: R-PRD-01, R-PRD-02, R-INF-01. Spec: docs/04-kien-truc.md §6.3, §9.5.

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

NB_NAME = "nb_gld_dim_product"
TARGET = "gld.dim_product"
SPEC = get_gold(TARGET)
PRIO = " ".join(f"WHEN '{s}' THEN {i}" for i, s in enumerate(SETTINGS["source_priority"], 1))
ATTRS = ["product_name", "brand", "category_code"]
CAT_COLS = ["category_path", "category_l1", "category_l2", "category_l3", "category_l4"]
PENDING = "Unknown (pending)"

def source_codes(full, iv, code_col):
    """(code, source_system) có trong orders: full ⇒ dòng hiện hành; incremental ⇒ CDF của orders_event."""
    if full:
        ev = latest_event(spark.table("slv.orders_event"))
    else:
        r = iv["slv.orders_event"]
        ev = read_changes("slv.orders_event", r["from"], r["to"]).where(
            "_change_type IN ('insert', 'update_postimage')")
    return ev.where(F.col(code_col).isNotNull()).select(code_col, "source_system").distinct()


def survivors():
    firsts = ", ".join(f"first_value({a}, true) OVER wnd AS {a}" for a in ATTRS)
    return spark.sql(f"""
        WITH cur AS (SELECT *, CASE source_system {PRIO} ELSE 99 END AS _prio FROM slv.v_products_current),
        w AS (SELECT product_code, {firsts},
                     concat_ws(',', array_sort(collect_set(CASE WHEN is_deleted THEN NULL ELSE source_system END)
                                               OVER p)) AS _live,
                     concat_ws(',', array_sort(collect_set(source_system) OVER p)) AS _all,
                     bool_and(is_deleted) OVER p AS is_deleted,
                     row_number() OVER (PARTITION BY product_code ORDER BY is_deleted, _prio) AS _rn
              FROM cur
              WINDOW p AS (PARTITION BY product_code),
                     wnd AS (PARTITION BY product_code ORDER BY is_deleted, _prio
                             ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))
        SELECT product_code, {", ".join(ATTRS)},
               CASE WHEN _live <> '' THEN _live ELSE _all END AS source_systems,
               is_deleted, false AS is_inferred
        FROM w WHERE _rn = 1""")


def categories():
    """R-PRD-02: một dòng mỗi category_code chưa xoá."""
    return spark.sql("""
        SELECT category_code AS _cat_code, category_lvl1 AS category_l1, category_lvl2 AS category_l2,
               category_lvl3 AS category_l3, category_lvl4 AS category_l4,
               nullif(concat_ws('/', category_lvl1, category_lvl2, category_lvl3, category_lvl4), '') AS category_path
        FROM (SELECT *, row_number() OVER (PARTITION BY category_code
                                           ORDER BY _snapshot_date DESC, _delivered_at_utc DESC) AS _rn
              FROM slv.v_categories_current WHERE NOT is_deleted) WHERE _rn = 1""")


def build(decision):
    full = decision["exec_mode"] == "full"
    reason = decision["reason"]
    iv = decision["input_versions"]
    try:
        codes = source_codes(full, iv, "product_code")
        codes.limit(1).collect()
    except CdfUnavailable:
        full, reason = True, "cdf_unavailable"
        codes = source_codes(True, iv, "product_code")
    surv = survivors()
    existing = spark.table(TARGET)
    agg = (codes.groupBy("product_code")
           .agg(F.concat_ws(",", F.array_sort(F.collect_set("source_system"))).alias("source_systems")))
    inferred = (agg.join(surv.select("product_code"), "product_code", "left_anti")
                .join(existing.select("product_code"), "product_code", "left_anti")
                .withColumn("product_name", F.lit(PENDING)).withColumn("brand", F.lit(None).cast("string"))
                .withColumn("category_code", F.lit(None).cast("string"))
                .withColumn("is_deleted", F.lit(False)).withColumn("is_inferred", F.lit(True)))
    parts = [surv, inferred]
    if not full:
        keep = (existing.where("is_inferred").where("product_sk <> -1")
                .join(surv.select("product_code"), "product_code", "left_anti")
                .select("product_code", "product_name", "brand", "category_code", "source_systems",
                        "is_deleted", "is_inferred"))
        parts.append(keep)
    unknown = spark.createDataFrame(
        [("#UNKNOWN", PENDING, "#", False, False)],
        "product_code string, product_name string, source_systems string, is_deleted boolean, is_inferred boolean")
    base = reduce_union(parts + [unknown])
    cat = categories()
    src = (base.join(cat, base["category_code"] == cat["_cat_code"], "left").drop("_cat_code")
           .withColumn("category_path", F.coalesce(F.col("category_path"), F.lit(PENDING))))
    cols = ATTRS + CAT_COLS + ["source_systems", "is_inferred", "is_deleted"]
    src = (src.withColumn("product_sk", F.when(F.col("product_code") == "#UNKNOWN", F.lit(-1).cast("bigint"))
                          .otherwise(F.expr(sk("product_code"))))
           .withColumn("_dim_hash", F.expr(record_hash(cols))))
    m = sync_table(TARGET, gold_audit(src, RUN_ID, SPEC["logic_version"]), ["product_sk"], "_dim_hash")
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

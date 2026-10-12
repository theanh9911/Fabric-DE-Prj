# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

p_run_id = ""
p_batch_id = ""
p_layer = "all"

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

import json

run_id = p_run_id or str(uuid.uuid4())
failures = []
results = []

def check_unique(table, keys, check_id):
    if not table_exists(table):
        return
    df = spark.table(table)
    dup = df.groupBy(*keys).count().where("count > 1")
    n = dup.count()
    sample = json.dumps([r.asDict() for r in dup.limit(5).collect()], default=str)
    status = log_check(run_id, p_batch_id, check_id, "dq", table, ",".join(keys),
                       0, n, 0, "critical", sample)
    results.append((check_id, status))

def check_not_null(table, columns, check_id):
    if not table_exists(table):
        return
    df = spark.table(table)
    condition = reduce(lambda a, b: a | b, [F.col(c).isNull() for c in columns])
    n = df.where(condition).count()
    status = log_check(run_id, p_batch_id, check_id, "dq", table, ",".join(columns),
                       0, n, 0, "critical")
    results.append((check_id, status))

if p_layer in ("all", "slv"):
    for source in SOURCES:
        table = f"slv.slv_{source['source_system']}_{source['entity']}"
        if table_exists(table):
            check_not_null(table, ["_bk"], f"{table}.business_key_not_null")
    if table_exists("slv.slv_orders_event"):
        check_unique("slv.slv_orders_event", ["source_system", "_bk", "order_status", "_updated_at_utc"],
                     "slv_orders_event.event_key_unique")

if p_layer in ("all", "gld"):
    for table, keys, required in [
        ("gld.gld_d_date", ["date_key"], ["date_key", "date_value"]),
        ("gld.gld_d_customer", ["customer_sk"], ["customer_sk"]),
        ("gld.gld_d_product", ["product_sk"], ["product_sk"]),
        ("gld.gld_d_salesman", ["salesman_sk"], ["salesman_sk"]),
        ("gld.gld_f_sales_line", ["order_no", "product_code"], ["order_no", "product_code", "date_key"]),
        ("gld.gld_a_sales_month", ["month_key", "product_sk", "customer_sk", "salesman_sk"], ["month_key"]),
    ]:
        check_unique(table, keys, f"{table}.unique")
        check_not_null(table, required, f"{table}.not_null")
    if table_exists("gld.gld_d_salesman"):
        overlap = spark.sql("""
          SELECT count(*) AS n FROM (
            SELECT source_system, salesman_code,
                   count_if(is_current) AS current_count
            FROM gld.gld_d_salesman GROUP BY source_system, salesman_code
            HAVING current_count <> 1
          ) x
        """).first()["n"]
        status = log_check(run_id, p_batch_id, "gld_d_salesman.one_current", "dq",
                           "gld.gld_d_salesman", "source_system,salesman_code", 0, overlap)
        results.append(("gld_d_salesman.one_current", status))

if table_exists("slv.quarantine"):
    open_count = spark.table("slv.quarantine").count()
    results.append(("slv.quarantine.open_count", open_count))

notebookutils.notebook.exit(json.dumps({"run_id": run_id, "checks": results,
                                        "quarantine_rows": open_count if table_exists("slv.quarantine") else 0}))

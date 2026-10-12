# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

p_run_id = ""
p_batch_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


%run nb_common

# CELL ********************

import json

run_id = p_run_id or str(uuid.uuid4())
checks = []

missing = [table for table in ["gld.gld_f_sales_line", "gld.gld_a_sales_month"]
           if not table_exists(table)]
for table in missing:
    check_id = f"required_table.{table}"
    log_check(run_id, p_batch_id, check_id, "recon", table, "table_exists",
              1, 0, 0, "critical", fail_fast=False)
    checks.append((check_id, "FAIL"))
if missing:
    raise RuntimeError(f"Cannot reconcile; required Gold tables are missing: {', '.join(missing)}")

if table_exists("gld.gld_f_sales_line") and table_exists("gld.gld_a_sales_month"):
    fact = spark.table("gld.gld_f_sales_line").withColumn("month_key", F.floor(F.col("date_key") / 100).cast("int"))
    expected = fact.groupBy("month_key").agg(F.sum("gross_amount").alias("gross"), F.count(F.lit(1)).alias("n"))
    actual = spark.table("gld.gld_a_sales_month").groupBy("month_key").agg(F.sum("gross_amount").alias("gross"), F.sum("sales_line_count").alias("n"))
    compare = expected.alias("e").join(actual.alias("a"), "month_key", "full")
    diffs = compare.where((F.coalesce(F.col("e.gross"), F.lit(0)) != F.coalesce(F.col("a.gross"), F.lit(0))) |
                          (F.coalesce(F.col("e.n"), F.lit(0)) != F.coalesce(F.col("a.n"), F.lit(0))))
    diff_count = diffs.count()
    sample = json.dumps([r.asDict() for r in diffs.limit(5).collect()], default=str)
    status = log_check(run_id, p_batch_id, "recon.fact_to_month", "recon",
                       "gld.gld_f_sales_line -> gld.gld_a_sales_month", "month_key",
                       0, diff_count, 0, "critical", sample)
    checks.append(("fact_to_month", status, diff_count))

if table_exists("meta.ingestion_batch"):
    source_rows = spark.table("meta.ingestion_batch").where(F.col("loaded_run_id") == run_id)
    for row in source_rows.collect():
        if row["rows_read"] is not None and row["rows_changed"] is not None and row["rows_quarantined"] is not None:
            accounted = int(row["rows_changed"]) + int(row["rows_unchanged"] or 0) + int(row["rows_quarantined"] or 0)
            status = log_check(run_id, row["batch_id"], f"delivery.{row['source_system']}.{row['entity']}.accounting",
                               "recon", row["landing_path"], row["delivery_id"], row["rows_read"], accounted)
            checks.append((row["entity"], status, accounted))

notebookutils.notebook.exit(json.dumps({"run_id": run_id, "checks": checks}))

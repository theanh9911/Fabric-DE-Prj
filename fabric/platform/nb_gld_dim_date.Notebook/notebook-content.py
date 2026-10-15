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

# # nb_gld_dim_date
# Rules: R-INF-01 (dòng không xác định). Spec: docs/04-kien-truc.md §6.1, §9.4.

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

NB_NAME = "nb_gld_dim_date"
from datetime import date

TARGET = "gld.dim_date"
SPEC = get_gold(TARGET)


def build(decision):
    start, end = SETTINGS["date_dim_start"], SETTINGS["date_dim_end"]
    days = spark.sql(f"SELECT explode(sequence(DATE'{start}', DATE'{end}', INTERVAL 1 DAY)) AS date_value")
    d = days.selectExpr(
        "CAST(date_format(date_value, 'yyyyMMdd') AS INT) AS date_key", "date_value",
        "year(date_value) AS year", "quarter(date_value) AS quarter",
        "CAST(date_format(date_value, 'yyyyMM') AS INT) AS month_key", "month(date_value) AS month",
        "date_format(date_value, 'MMMM') AS month_name", "day(date_value) AS day",
        "weekday(date_value) + 1 AS day_of_week", "date_format(date_value, 'EEEE') AS day_name",
        "weekofyear(date_value) AS iso_week", "weekday(date_value) >= 5 AS is_weekend")
    unknown = spark.createDataFrame([(-1, date(1900, 1, 1))], "date_key int, date_value date")
    out = gold_audit(d.unionByName(unknown, allowMissingColumns=True), RUN_ID, SPEC["logic_version"])
    m = overwrite_table(TARGET, out)
    return {"rows_in": m["rows_inserted"], "rows_inserted": m["rows_inserted"]}

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

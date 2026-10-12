# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
# META   "dependencies": {}
# META }

# CELL ********************

# MAGIC %%configure -f
# MAGIC { "defaultLakehouse": { "name": "lh_platform" } }

# PARAMETERS CELL ********************

p_optimize = True

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_common

tables = []
for schema_name in ["slv", "gld"]:
    try:
        tables.extend([f"{schema_name}.{r['tableName']}" for r in spark.sql(f"SHOW TABLES IN {schema_name}").collect()
                       if not r["tableName"].startswith("_")])
    except Exception:
        pass

optimized = []
if p_optimize:
    for table in sorted(set(tables)):
        if table.endswith("quarantine"):
            continue
        try:
            spark.sql(f"OPTIMIZE {safe_table_name(table)}")
            optimized.append(table)
        except Exception as exc:
            print(f"OPTIMIZE skipped for {table}: {exc}")

notebookutils.notebook.exit(json.dumps({"optimized_tables": optimized}))

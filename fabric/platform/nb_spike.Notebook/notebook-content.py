# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_spike (T0.2 — tạm, xoá ở T8.4)
# Kiểm giả định runtime K01–K05, K09, K10 của docs/06-tasks.md. Mỗi spike một cell, kết quả gom vào `RESULTS`;
# cell cuối in bảng để dán vào docs/DECISIONS.md §3. Dùng schema `tst`, xoá ở cuối.
#
# K06, K07, K08, K11, K12 cần pipeline (`pl_spike`) — dựng bằng giao diện Fabric, không kiểm được ở notebook.
# K03 cần tham số `p_source_workspace_id`, `p_drop_lakehouse_id` (lấy từ URL của workspace/lakehouse Source).

# PARAMETERS CELL ********************

p_source_workspace_id = ""
p_drop_lakehouse_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import traceback

RESULTS = {}


def record(key, fn):
    try:
        RESULTS[key] = {"ok": True, "detail": fn()}
    except Exception as e:  # spike: ghi lại mọi lỗi để biết giả định nào sai
        RESULTS[key] = {"ok": False, "detail": f"{type(e).__name__}: {str(e)[:400]}"}


spark.sql("DROP SCHEMA IF EXISTS tst CASCADE")
spark.sql("CREATE SCHEMA tst")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def k01():
    spark.sql("CREATE TABLE tst.k01 (id INT, v STRING) USING DELTA "
              "TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    spark.sql("INSERT INTO tst.k01 VALUES (1,'a'),(2,'b'),(3,'c')")                      # v1
    spark.sql("MERGE INTO tst.k01 t USING (SELECT 1 AS id, 'a2' AS v) s ON t.id = s.id "
              "WHEN MATCHED THEN UPDATE SET t.v = s.v")                                  # v2
    spark.sql("DELETE FROM tst.k01 WHERE id = 3")                                        # v3
    spark.sql("SELECT * FROM tst.k01").write.mode("overwrite").option("overwriteSchema", "false") \
        .saveAsTable("tst.k01")                                                          # v4
    rows = spark.sql("SELECT _commit_version AS v, _change_type AS t, count(*) AS n "
                     "FROM table_changes('tst.k01', 1, 4) GROUP BY 1, 2 ORDER BY 1, 2").collect()
    return [(r.v, r.t, r.n) for r in rows]


record("K01", k01)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def k02():
    spark.sql("CREATE TABLE tst.k02 (id INT, v STRING) USING DELTA")
    spark.sql("INSERT INTO tst.k02 VALUES (1,'a'),(2,'b'),(3,'c')")
    spark.sql("MERGE INTO tst.k02 t USING (SELECT 1 AS id, 'a2' AS v UNION ALL SELECT 4, 'd') s ON t.id = s.id "
              "WHEN MATCHED THEN UPDATE SET t.v = s.v "
              "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v) "
              "WHEN NOT MATCHED BY SOURCE THEN DELETE")
    return sorted(tuple(r) for r in spark.sql("SELECT id, v FROM tst.k02").collect())  # mong đợi [(1,a2),(4,d)]


record("K02", k02)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def k03():
    if not (p_source_workspace_id and p_drop_lakehouse_id):
        return "bỏ qua: chưa truyền p_source_workspace_id / p_drop_lakehouse_id"
    path = f"abfss://{p_source_workspace_id}@onelake.dfs.fabric.microsoft.com/{p_drop_lakehouse_id}/Files/inbound"
    entries = notebookutils.fs.ls(path)
    first = entries[0] if entries else None
    return {"count": len(entries),
            "attrs": [a for a in dir(first) if not a.startswith("_")] if first else None,
            "sample": {"name": first.name, "size": first.size, "modifyTime": first.modifyTime} if first else None}


record("K03", k03)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def k04():
    base = "Files/_spike"
    notebookutils.fs.rm(base, True)
    notebookutils.fs.put(f"{base}/staging/d1/part.txt", "x", True)
    ok = notebookutils.fs.mv(f"{base}/staging/d1", f"{base}/landing/b/s/e/run=d1", True)
    return {"mv_returned": ok,
            "staging_exists": notebookutils.fs.exists(f"{base}/staging/d1"),
            "landing_files": [f.name for f in notebookutils.fs.ls(f"{base}/landing/b/s/e/run=d1")]}


record("K04", k04)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def k05():
    out = {"spark.version": spark.version,
           "ansi": spark.conf.get("spark.sql.ansi.enabled")}
    out["try_to_timestamp"] = spark.sql(
        "SELECT try_to_timestamp('3/15/2025 9:30', 'M/d/yyyy H:mm') AS a, "
        "try_to_timestamp('abc', 'M/d/yyyy H:mm') AS b").collect()[0].asDict()
    try:
        out["sum_bigint_overflow"] = str(spark.sql(
            "SELECT sum(x) AS s FROM VALUES (CAST(9223372036854775807 AS BIGINT)), (CAST(1 AS BIGINT)) AS t(x)"
        ).collect()[0].s)
    except Exception as e:
        out["sum_bigint_overflow"] = f"ERROR {type(e).__name__}"
    return out


record("K05", k05)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def k09():
    h = spark.sql("DESCRIBE HISTORY tst.k01").select("version", "operation", "operationMetrics").collect()
    return [(r.version, r.operation, dict(r.operationMetrics)) for r in h]


record("K09", k09)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def k10():
    out = {}
    spark.sql("CREATE TABLE tst.k10 (id INT NOT NULL, v STRING) USING DELTA")
    spark.sql("INSERT INTO tst.k10 VALUES (1, 'a')")
    spark.createDataFrame([(2, "b")], "id INT, v STRING").write.mode("overwrite") \
        .option("overwriteSchema", "false").saveAsTable("tst.k10")
    out["overwrite_keeps_not_null"] = [(f.name, f.nullable) for f in spark.table("tst.k10").schema.fields]
    try:
        spark.createDataFrame([(None, "x")], "id INT, v STRING").write.mode("append").saveAsTable("tst.k10")
        out["null_into_not_null"] = "KHÔNG báo lỗi"
    except Exception as e:
        out["null_into_not_null"] = f"lỗi: {type(e).__name__}: {str(e)[:200]}"
    return out


record("K10", k10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DROP SCHEMA IF EXISTS tst CASCADE")
notebookutils.fs.rm("Files/_spike", True)
print(json.dumps(RESULTS, indent=2, ensure_ascii=False, default=str))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

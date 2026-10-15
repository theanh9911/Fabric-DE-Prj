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
# K03 tự tìm workspace và lakehouse Source theo tên (`p_source_workspace`, `p_drop_lakehouse`); cũng là thử `resolve_source_ids`.

# MARKDOWN ********************

# ## Dựng `pl_spike` (K06, K07, K08, K11, K12)
# Tạo pipeline `pl_spike` trong CompanyA-DataPlatform. Tham số của notebook activity (`nb_spike`): `p_fail`, `p_exit_items`, `p_exit_pad`.
#
# 1. **Activity `nb`** (Notebook → `nb_spike`, `p_fail = false`). Ghi lại `exitValue`: có `source_workspace_id`, `drop_lakehouse_id`, `sources`.
# 2. **K08**: ForEach, `Items` = `@json(activity('nb').output.result.exitValue).sources`; bên trong một activity Set variable ghi `@item().id`. Thử tăng `p_exit_items` (9 → 100 → 1000) với `p_exit_pad = 400` cho tới khi hỏng: ghi kích thước exit value tối đa.
# 3. **K11a**: Copy. Nguồn = SQL database `sqldb_erp_wholesale` (Connection), truy vấn `SELECT * FROM dbo.customers`. Đích = `lh_platform`, Files, thư mục `_staging/spike_a`, Parquet. Ghi: `rowsRead`; thư mục đích tạo ra thế nào.
# 4. **K11b**: Copy. Nguồn = Lakehouse `lh_retail_drop` (Connection), Files, thư mục `inbound/retail/orders`, wildcard `*.csv`, lọc *Last modified* theo khoảng; định dạng Binary. Đích = `lh_platform`, `_staging/spike_b`, **Flatten hierarchy**. Ghi: có lọc đúng khoảng không.
# 5. **K11c**: lặp K11b nhưng workspace ID và lakehouse ID lấy từ `@json(activity('nb').output.result.exitValue).source_workspace_id` / `.drop_lakehouse_id` (dynamic content). Ghi: dùng được không, cần Connection gì.
# 6. **K11e**: Copy K11b với khoảng *Last modified* không có file nào. Ghi: có tạo thư mục đích không, `rowsRead`/`filesRead`.
# 7. **K06**: Settings của pipeline → Concurrency = 1; Git sync giữ giá trị này không (xem `pipeline-content.json`). Chạy hai lượt cách nhau vài giây; ghi lượt hai bị xếp hàng, huỷ hay chạy song song.
# 8. **K07**: thêm activity Teams (Activities → Notifications). Ghi: có trong Data pipeline không; cần Connection/quyền gì; gửi vào chat hay kênh.
# 9. **K12**: ba activity nối `nb (p_fail = true)` → `a2` (Wait 1s) → `a3` (Wait 1s). Thêm `catch` (Set variable) có `dependsOn` `a3` với điều kiện **Failed** và **Skipped**. Chạy: `catch` có chạy không; lấy được lỗi bằng `@activity('nb').error.message` không. Lặp lại với lỗi ở `a2`.

# PARAMETERS CELL ********************

p_source_workspace = "CompanyA-Source"
p_drop_lakehouse = "lh_retail_drop"
p_fail = False
p_exit_items = 9
p_exit_pad = 400

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import traceback

if p_fail:
    raise RuntimeError("spike deliberate failure (K12)")

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
    import sempy.fabric as fabric
    ws_id = fabric.resolve_workspace_id(p_source_workspace)
    lh_id = fabric.resolve_item_id(p_drop_lakehouse, "Lakehouse", ws_id)
    root = f"abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{lh_id}/Files/inbound"

    def walk(path, depth):
        out = []
        for f in notebookutils.fs.ls(path):
            out.append({"path": f.path.split("/Files/inbound", 1)[-1], "isDir": f.isDir,
                        "size": f.size, "modifyTime": f.modifyTime})
            if f.isDir and depth > 1:
                out.extend(walk(f.path, depth - 1))
        return out

    tree = walk(root, 4)
    files = [x for x in tree if not x["isDir"]]
    return {"resolved": {"workspace_id": ws_id, "lakehouse_id": lh_id},
            "dirs": [x["path"] for x in tree if x["isDir"]],
            "file_count": len(files), "files_sample": files[:5]}

record("K03", k03)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def k04():
    base = "Files/_spike"
    if notebookutils.fs.exists(base):
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
if notebookutils.fs.exists("Files/_spike"):
    notebookutils.fs.rm("Files/_spike", True)
print(json.dumps(RESULTS, indent=2, ensure_ascii=False, default=str))

# Giá trị trả về cho pipeline (K08: dùng làm items của ForEach; K11c: ID động cho Copy).
import sempy.fabric as fabric

_ws = fabric.resolve_workspace_id(p_source_workspace)
_lh = fabric.resolve_item_id(p_drop_lakehouse, "Lakehouse", _ws)
notebookutils.notebook.exit(json.dumps({
    "source_workspace_id": _ws,
    "drop_lakehouse_id": _lh,
    "sources": [{"id": f"src{i}", "query": "SELECT * FROM dbo.t " + "x" * p_exit_pad}
                for i in range(p_exit_items)],
}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

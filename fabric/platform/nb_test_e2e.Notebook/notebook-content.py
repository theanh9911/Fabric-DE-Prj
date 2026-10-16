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

# # nb_test_e2e
# Kiểm sau diễn tập sự cố (docs/07-van-hanh-kiem-thu.md §3.3, §5). Đọc `meta`, `slv`, `gld` và `lh_sim` (log của simulator); chỉ RC-06 ghi một dòng `meta.check_result`. Mỗi kịch bản một hàm `test_scenario_<tên>`: nếu kịch bản chưa được tiêm
# (`sim_inject_log` không có dòng) thì `SKIP`. Cộng `test_current_matches_view` và RC-06.

# PARAMETERS CELL ********************

p_source_workspace = "CompanyA-Source"
p_sim_lakehouse = "lh_sim"

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

NB_NAME = "nb_test_e2e"
import json


class Skip(Exception):
    pass


def sim_table_df(name):
    import sempy.fabric as fabric
    ws = fabric.resolve_workspace_id(p_source_workspace)
    lh = fabric.resolve_item_id(p_sim_lakehouse, "Lakehouse", ws)
    return spark.read.format("delta").load(f"abfss://{ws}@onelake.dfs.fabric.microsoft.com/{lh}/Tables/{name}")


def injected(scenario):
    try:
        rows = sim_table_df("sim_inject_log").where(F.col("scenario") == scenario).orderBy(
            F.col("injected_at").desc()).limit(1).collect()
    except Exception as e:  # noqa: BLE001
        raise Skip(f"không đọc được sim_inject_log: {e}") from e
    if not rows:
        raise Skip(f"chưa tiêm kịch bản {scenario}")
    r = rows[0].asDict()
    r["detail"] = json.loads(r["detail_json"])
    return r


def scalar(sql):
    return spark.sql(sql).collect()[0][0]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def test_current_matches_view():
    cur = latest_event(spark.table("slv.orders_event"))
    a = cur.agg(F.count(F.lit(1)).alias("n"), F.sum(F.xxhash64("_record_hash").cast("decimal(38,0)")).alias("h")
                ).collect()[0]
    b = spark.table("slv.v_orders_current").agg(
        F.count(F.lit(1)).alias("n"), F.sum(F.xxhash64("_record_hash").cast("decimal(38,0)")).alias("h")).collect()[0]
    assert a["n"] == b["n"] and a["h"] == b["h"], f"latest_event {tuple(a)} khác view {tuple(b)}"


def test_current_unique_per_line():
    dup = scalar("SELECT count(*) - count(DISTINCT source_system, order_no, product_code) FROM slv.v_orders_current")
    assert dup == 0, f"{dup} dòng hàng có nhiều hơn một dòng hiện hành"


def test_gold_matches_silver():
    a = spark.sql("SELECT count(*) n, coalesce(sum(quantity * price), 0) amt FROM slv.v_orders_current").collect()[0]
    b = spark.sql("SELECT count(*) n, coalesce(sum(quantity * price), 0) amt FROM gld.fct_order_line").collect()[0]
    assert (a["n"], a["amt"]) == (b["n"], b["amt"]), f"RC-05: silver {tuple(a)} khác gold {tuple(b)}"


def test_scenario_late_arrival():
    n = scalar("SELECT count(*) FROM gld.fct_order_line WHERE order_date_key BETWEEN 20260101 AND 20260131 "
               "AND first_pending_at_utc >= TIMESTAMP'2026-01-15 00:00:00'")
    if n == 0:
        raise Skip("không có dòng đến muộn ngày 2026-01-15 trong dữ liệu đã nạp")
    assert scalar("SELECT count(*) FROM gld.fct_order_line WHERE order_date_key = -1") == 0


def test_scenario_order_update():
    n = scalar("SELECT count(*) FROM gld.fct_order_line WHERE status_event_count > 1")
    assert n > 0, "không có dòng hàng nào có nhiều hơn một sự kiện trạng thái"


def test_scenario_dirty_data():
    allowed = {"missing_business_key", "invalid_updated_at", "invalid_order_date", "unknown_order_status",
               "invalid_quantity", "invalid_price", "future_order_date", "invalid_tax_rate", "ambiguous_date",
               "negative_quantity", "negative_price", "unmapped_value", "duplicate_key_in_snapshot", "self_manager",
               "missing_required_value", "invalid_timestamp", "invalid_date", "invalid_number", "unknown_value"}
    got = {r[0] for r in spark.sql("SELECT DISTINCT reason FROM slv.quarantine").collect()}
    assert got <= allowed, f"mã lý do ngoài danh mục: {sorted(got - allowed)}"


def test_scenario_file_resend():
    r = injected("file_resend")
    n = scalar("SELECT count(*) FROM (SELECT _event_key FROM slv.orders_event GROUP BY _event_key HAVING count(*) > 1)")
    assert n == 0, "file gửi lại làm nhân đôi sự kiện"
    row = spark.sql(f"SELECT quantity FROM slv.orders_event WHERE order_no = '{r['detail']['order_no']}' "
                    "ORDER BY _delivered_at_utc DESC LIMIT 1").collect()
    assert row and str(row[0][0]) == "999.00", f"dòng sửa chưa được cập nhật: {row}"


def test_scenario_duplicate_file():
    injected("duplicate_file")
    n = scalar("SELECT count(*) FROM meta.check_result WHERE check_id = 'DQ-ING-01' AND status = 'WARN'")
    assert n >= 1, "DQ-ING-01 chưa cảnh báo file trùng nội dung"


def test_scenario_schema_change():
    injected("schema_change")
    n = scalar("SELECT count(*) FROM meta.delivery_log WHERE array_contains(schema_added, 'discount_amount')")
    assert n >= 1, "delivery_log chưa ghi schema_added = discount_amount"
    assert scalar("SELECT count(*) FROM meta.check_result WHERE check_id = 'DQ-SLV-06' AND status = 'WARN'") >= 1


def test_scenario_org_change():
    r = injected("org_change")
    code = r["detail"]["salesman_code"].strip().upper()
    n = scalar(f"SELECT count(*) FROM gld.dim_salesman WHERE salesman_code = '{code}' AND source_system = 'wholesale'")
    assert n >= 2, f"{code} chỉ có {n} phiên bản"
    assert scalar(f"SELECT count(*) FROM slv.sales_hierarchy_history WHERE salesman_code = '{code}' "
                  "AND _change_kind = 'UPDATE'") >= 1


def test_scenario_master_delete():
    r = injected("master_delete")
    key = str(r["detail"]["deleted_key"]).strip().upper()
    entity = r["entity"].split(".", 1)[1]
    col = {"products": "product_code", "customers": "customer_code",
           "sales_hierarchy": "salesman_code", "categories": "category_code"}[entity]
    n = scalar(f"SELECT count(*) FROM slv.{entity}_history WHERE {col} = '{key}' AND _change_kind = 'DELETE'")
    assert n >= 1, f"không có dòng DELETE cho {key}"


def test_scenario_missing_and_partial_file():
    injected("missing_file")
    assert scalar("SELECT count(*) FROM meta.check_result WHERE check_id = 'DQ-OPS-01' AND status = 'WARN'") >= 1


def test_rc06_simulator_vs_landing():
    rel = (sim_table_df("sim_release_log").where("source_system = 'retail' AND entity = 'orders'")
           .agg(F.sum("rows_released")).collect()[0][0])
    got = scalar("SELECT coalesce(sum(rows_landed), 0) FROM meta.v_delivery_state "
                 "WHERE status = 'LOADED' AND source_system = 'retail' AND entity = 'orders'")
    if rel is None:
        raise Skip("sim_release_log trống")
    st = log_check(new_id(), None, "RC-06", "recon", "reconcile", "retail.orders", None, rel, got, 0, "warning",
                   message="rows_released của simulator so với rows_landed của platform")
    assert st in ("PASS", "WARN")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results = []
for name, fn in sorted((n, f) for n, f in globals().items() if n.startswith("test_") and callable(f)):
    try:
        fn()
        results.append((name, "PASS", ""))
    except Skip as s:
        results.append((name, "SKIP", str(s)[:300]))
    except Exception as exc:  # noqa: BLE001
        results.append((name, "FAIL", str(exc)[:500]))
display(spark.createDataFrame(results, "test string, status string, error string"))
failed = [r[0] for r in results if r[1] == "FAIL"]
if failed:
    raise AssertionError(f"{len(failed)} test(s) failed: {failed}")
print(f"{sum(r[1] == 'PASS' for r in results)} PASS, {sum(r[1] == 'SKIP' for r in results)} SKIP")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

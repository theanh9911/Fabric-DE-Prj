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

# # nb_test_unit
# Test đơn vị của `nb_std`, `nb_config`, `nb_common` (docs/07-van-hanh-kiem-thu.md §3.2). Chỉ dùng schema `tst`
# (tạo ở đầu, xoá ở cuối); không đọc/ghi `meta`, `slv`, `gld`. Mỗi test là hàm `test_*` dùng `assert` có thông điệp.

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

NB_NAME = "nb_test_unit"
import copy
import datetime as dt
from decimal import Decimal

spark.sql("DROP SCHEMA IF EXISTS tst CASCADE")
spark.sql("CREATE SCHEMA tst")


def val(expr):
    return spark.sql(f"SELECT {expr} AS v").collect()[0]["v"]


def s(expr):
    return val(f"CAST(({expr}) AS STRING)")


def q(text):
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- nb_std ------------------------------------------------------------------------------------------------

def test_std__null_if_blank():
    assert val(null_if_blank("'  NULL '")) is None, "'  NULL ' phải thành NULL"
    assert val(null_if_blank("'   '")) is None
    assert val(null_if_blank("' x '")) == "x"


def test_std__code_text_title():
    assert val(std_code("' cus 001'")) == "CUS001"
    assert val(std_text("'a   b'")) == "a b"
    assert val(std_title("'viet NAM'")) == "Viet Nam"


def test_std__parse_ts():
    assert s(parse_ts("'3/15/2025 9:30'", ["M/d/yyyy H:mm"], "Asia/Ho_Chi_Minh")) == "2025-03-15 02:30:00"
    assert val(parse_ts("'abc'", ["M/d/yyyy H:mm"], "UTC")) is None
    assert s(parse_ts("'2025-03-15 09:30:00'", ["M/d/yyyy H:mm", "yyyy-MM-dd HH:mm:ss"], "UTC")) == "2025-03-15 09:30:00"


def test_std__parse_date():
    fm = ["M/d/yyyy", "d/M/yyyy"]
    assert s(parse_date("'25/3/2025'", fm)) == "2025-03-25"
    assert s(parse_date("'3/4/2025'", fm)) == "2025-03-04"
    assert val(parse_date("'xx'", fm)) is None


def test_std__is_ambiguous_dm():
    assert val(is_ambiguous_dm("'3/4/2025'")) is True
    assert val(is_ambiguous_dm("'4/4/2025'")) is False
    assert val(is_ambiguous_dm("'13/4/2025'")) is False
    assert val(is_ambiguous_dm("'2025-03-04'")) is False


def test_std__parse_decimal():
    assert s(parse_decimal("'1,234.5'")) == "1234.50"
    assert val(parse_decimal("'abc'")) is None
    assert s(parse_decimal("'-5'")) == "-5.00"


def test_std__dict():
    assert val(dict_key("' Delivred '")) == "DELIVRED"
    assert val(std_dict("'delivered '", ORDER_STATUS)) == "Delivered"
    assert val(std_dict("'xyz'", ORDER_STATUS)) is None
    assert val(std_dict("'  levis '", BRAND)) == "Levi's"


def test_std__string_literal_escape():
    assert val(std_dict("'x'", {"X": "Levi's"})) == "Levi's", "literal có dấu ' phải được escape"
    assert val(std_dict("'x'", {"X": "a\\b"})) == "a\\b"


def test_std__lookup_expr():
    assert val(lookup_expr("'Delivered'", STATUS_RANK, "int")) == 4
    assert val(lookup_expr("'Unknown'", STATUS_RANK, "int")) is None


def test_std__parse_tax_rate():
    for raw in ("5%", "5", "0.05", "five percent"):
        assert s(parse_tax_rate(q(raw), TAX_WORDS)) == "0.050000", raw
    assert val(parse_tax_rate("'150%'", TAX_WORDS)) is None
    assert val(parse_tax_rate("'abc'", TAX_WORDS)) is None


def test_std__record_hash_sk_dates():
    df = spark.sql("SELECT 'x' AS a, CAST(NULL AS STRING) AS b, '' AS c, 'x' AS a2")
    h1 = df.select(F.expr(record_hash(["a", "b"])).alias("h")).collect()[0]["h"]
    h2 = df.select(F.expr(record_hash(["b", "a"])).alias("h")).collect()[0]["h"]
    h3 = df.select(F.expr(record_hash(["a", "c"])).alias("h")).collect()[0]["h"]
    assert h1 == h2, "đổi thứ tự cột không được đổi hash"
    assert h1 != h3, "NULL phải khác chuỗi rỗng"
    assert val(sk("'a'", "'b'")) == val(sk("'a'", "'b'"))
    assert s(business_date("TIMESTAMP'2025-03-14 18:00:00'", "Asia/Ho_Chi_Minh")) == "2025-03-15"
    assert val(date_key("DATE'2025-03-15'")) == 20250315
    assert val(date_key("CAST(NULL AS DATE)")) == -1


def test_std__schema_drift():
    c = {"columns": [{"name": "a", "required": True}, {"name": "b", "required": False}]}
    added, missing, req = schema_drift([" A ", "z"], c)
    assert added == ["z"] and missing == ["b"] and req == [], (added, missing, req)
    assert schema_drift(["b"], c)[2] == ["a"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- conform_sql: fixture 10 dòng của 05 §3.3 -------------------------------------------------------------

def test_std__conform_sql_fixture():
    c = get_contract("retail.orders")
    spec = get_silver("slv.orders_event")
    cols = ("order_no", "customer_code", "order_date", "order_status", "product_code", "salesman_code",
            "quantity", "price", "tax_rate", "created_at", "updated_at", "extra_col")
    ok = ["O1", " c1", "2026-01-15", "Delivered", "P1", "S1", "10", "2.5", "5%", "2026-01-15 08:00:00",
          "2026-01-15 09:00:00", "x"]

    def row(**kw):
        r = dict(zip(cols, ok))
        r.update(kw)
        return tuple(r[k] for k in cols)

    rows = [row(),                                          # 1 ok
            row(order_no=" "),                              # 2 khoá rỗng
            row(updated_at="abc"),                          # 3 updated_at sai
            row(order_status="Pendng"),                     # 4 sai chính tả nhưng có trong từ điển
            row(order_status="Shiped"),                     # 5 không có trong từ điển
            row(tax_rate="five percent"),                   # 6 thuế dạng chữ
            row(tax_rate="150%"),                           # 7 thuế vượt khoảng
            row(order_date="3/4/2025"),                     # 8 ngày mơ hồ
            row(order_date="2027-05-01"),                   # 9 ngày tương lai
            row(quantity="-5")]                             # 10 số âm
    schema = ", ".join(f"{k} string" for k in cols) + ", _row_no bigint"
    df = spark.createDataFrame([r + (i + 1,) for i, r in enumerate(rows)], schema)
    df = (df.withColumn("source_system", F.lit("retail")).withColumn("_batch_id", F.lit("20260115"))
            .withColumn("_delivery_id", F.lit("d1")).withColumn("_delivered_at_utc", F.current_timestamp())
            .withColumn("_snapshot_date", F.lit(None).cast("date")).withColumn("_source_file", F.lit("f.csv")))
    df.createOrReplaceTempView("tst_src")
    avail = {x.lower() for x in df.columns}
    out = spark.sql(conform_sql(c, spec, DICTIONARIES, SETTINGS["future_order_days"], "tst_src",
                                available_columns=avail)).collect()
    got = {int(r["_row_no"]): (r["_reject_reason"], sorted(r["_dq_flags"] or [])) for r in out}
    want = {1: (None, []), 2: ("missing_business_key", []), 3: ("invalid_updated_at", []), 4: (None, []),
            5: ("unknown_order_status", []), 6: (None, []), 7: (None, ["invalid_tax_rate"]),
            8: (None, ["ambiguous_date"]), 9: ("future_order_date", []), 10: (None, ["negative_quantity"])}
    assert got == want, f"got={got}"
    r1 = [r for r in out if int(r["_row_no"]) == 1][0]
    assert r1["customer_code"] == "C1" and r1["order_status"] == "Delivered" and r1["status_rank"] == 4
    assert r1["tax_rate"] == Decimal("0.05") and r1["quantity"] == Decimal("10.00")
    assert r1["_bk"] == "retail|O1|P1", r1["_bk"]
    r4 = [r for r in out if int(r["_row_no"]) == 4][0]
    assert r4["order_status"] == "Pending" and r4["order_status_raw"] == "Pendng"
    r8 = [r for r in out if int(r["_row_no"]) == 8][0]
    assert str(r8["order_date"]) == "2025-04-03", "d/M/yyyy: 3/4/2025 = ngày 3 tháng 4"


def test_std__conform_sql_missing_column():
    c = get_contract("retail.orders")
    spec = get_silver("slv.orders_event")
    df = spark.createDataFrame([("O1", "P1", "Delivered", "2026-01-15", "2026-01-15 09:00:00")],
                               "order_no string, product_code string, order_status string, order_date string, "
                               "updated_at string")
    df = (df.withColumn("source_system", F.lit("retail")).withColumn("_batch_id", F.lit("20260115"))
            .withColumn("_delivery_id", F.lit("d1")).withColumn("_delivered_at_utc", F.current_timestamp())
            .withColumn("_snapshot_date", F.lit(None).cast("date")).withColumn("_source_file", F.lit("f.csv"))
            .withColumn("_row_no", F.lit(1)))
    df.createOrReplaceTempView("tst_src2")
    r = spark.sql(conform_sql(c, spec, DICTIONARIES, 1, "tst_src2",
                              available_columns={"order_no", "product_code", "order_status", "order_date",
                                                 "updated_at"})).collect()[0]
    assert r["customer_code"] is None and r["quantity"] is None
    assert r["_reject_reason"] == "invalid_quantity", r["_reject_reason"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- validate_config: mỗi luật V01–V10 một cấu hình hỏng ----------------------------------------------------

def _expect_config_error(code, mutate):
    saved = {k: copy.deepcopy(globals()[k]) for k in ("SILVER", "GOLD", "CHECKS", "ORDER_STATUS", "STATUS_RANK",
                                                      "SETTINGS", "CONTRACTS", "DICTIONARIES")}
    try:
        mutate()
        try:
            validate_config()
        except ValueError as e:
            assert code in str(e), f"{code} không có trong: {e}"
            return
        raise AssertionError(f"{code}: cấu hình hỏng nhưng validate_config không báo lỗi")
    finally:
        globals().update(saved)


def test_config__valid():
    out = validate_config()
    assert out["contracts"] == 9 and out["silver"] == 5 and out["gold"] == 5, out


def test_config__V01():
    _expect_config_error("V01", lambda: SILVER.append(copy.deepcopy(SILVER[0])))


def test_config__V02():
    def m():
        CONTRACTS["retail.orders"]["silver_table"] = "slv.nope"
    _expect_config_error("V02", m)


def test_config__V03():
    def m():
        SILVER[0]["columns"][0]["std"] = "nope"
    _expect_config_error("V03", m)


def test_config__V04():
    def m():
        SILVER[0]["columns"][2]["source"] = "no_such_column"
    _expect_config_error("V04", m)


def test_config__V05():
    def m():
        SILVER[0]["business_keys"] = ["order_no", "no_such"]
    _expect_config_error("V05", m)


def test_config__V06():
    def m():
        GOLD[0]["inputs"] = [dict(table="gld.fct_order_line", read="full")]
    _expect_config_error("V06", m)


def test_config__V07():
    def m():
        SILVER[0]["logic_version"] = 0
    _expect_config_error("V07", m)


def test_config__V08():
    def m():
        CHECKS.extend([dict(check_id="X", object="slv.orders_event", type="unique", severity="critical",
                            scope="touched"),
                       dict(check_id="X", object="slv.orders_event", type="unique", severity="critical",
                            scope="touched")])
    _expect_config_error("V08", m)


def test_config__V09():
    def m():
        ORDER_STATUS["ZZ"] = "Zzz"
    _expect_config_error("V09", m)


def test_config__V10():
    def m():
        del SETTINGS["cdf_max_age_days"]
    _expect_config_error("V10", m)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- preflight, plan_ingest, decide_run ---------------------------------------------------------------------

def test_plan__preflight():
    ok = preflight("normal", "2026-01-15", "", "ALL", False)
    assert ok["batch_id"] == "20260115" and ok["mode"] == "normal"
    r = preflight("rerun", "", "", "ALL", True)
    assert r["check_scope"] == "full" and r["load_date"] is None
    for args, code in [(("x", "", "", "ALL", False), "PF01"), (("normal", "15/01/2026", "", "ALL", False), "PF02"),
                       (("rerun", "", "", "ALL", False), "PF03"), (("normal", "", "", "ALL", True), "PF04"),
                       (("rerun", "2026-01-01", "", "ALL", True), "PF05"),
                       (("normal", "2026-01-01", "2026-01-02", "ALL", False), "PF06"),
                       (("normal", "2026-01-01", "", "foo.bar", False), "PF07")]:
        try:
            preflight(*args)
        except ValueError as e:
            assert str(e).startswith(code), (args, str(e))
        else:
            raise AssertionError(f"{code} không báo lỗi")


def _ms(y, m, d, h=0, mi=0):
    return int(dt.datetime(y, m, d, h, mi, tzinfo=dt.timezone.utc).timestamp() * 1000)


def test_plan__plan_ingest():
    start = dt.datetime(2026, 1, 16, 0, 5, tzinfo=dt.timezone.utc)
    listings = {"retail.orders": [
        {"name": "orders_20260115.csv", "size": 100, "modify_ms": _ms(2026, 1, 15, 23)},
        {"name": "orders_20260116.csv", "size": 50, "modify_ms": _ms(2026, 1, 16, 0, 4)}],
        "retail.customers": [
        {"name": "customers_20260110.csv", "size": 10, "modify_ms": _ms(2026, 1, 10, 1)},
        {"name": "customers_20260114.csv", "size": 12, "modify_ms": _ms(2026, 1, 14, 1)}]}
    seq = iter(range(1000))
    p = plan_ingest(active_contracts(), "2026-01-15", {("wholesale", "orders"): "2026-01-15 00:00:00"}, set(),
                    listings, start, "20260115", id_fn=lambda: f"d{next(seq)}")
    by = {x["source_id"]: x for x in p["sources"]}
    wo = by["wholesale.orders"]
    assert wo["window_start"] == "2026-01-15 00:00:00" and wo["window_end"] == "2026-01-16 00:00:00", wo
    assert wo["query"].startswith("SELECT * FROM dbo.orders WHERE [updated_at] >= '2026-01-15 00:00:00'")
    assert by["wholesale.customers"]["query"] == "SELECT * FROM dbo.customers"
    assert by["wholesale.customers"]["snapshot_date"] == "2026-01-15"
    ro = by["retail.orders"]
    assert [f["name"] for f in ro["expected_files"]] == ["orders_20260115.csv"], "file trong cửa sổ settle phải bị bỏ"
    rc = by["retail.customers"]
    assert rc["snapshot_date"] == "2026-01-14" and [f["name"] for f in rc["expected_files"]] == ["customers_20260114.csv"]
    assert [x["file_name"] for x in p["superseded"]] == ["customers_20260110.csv"]
    assert {"source_id": "reference.categories", "reason": "no_new_files"} in p["no_data"]
    # đã xử lý thì không vào kế hoạch lại; chỉ còn file settle ⇒ settle_pending
    done = {("retail", "orders", "orders_20260115.csv", 100, _ms(2026, 1, 15, 23) // 1000)}
    p2 = plan_ingest(active_contracts(), "2026-01-15", {}, done, listings, start, "20260115")
    assert "retail.orders" not in {x["source_id"] for x in p2["sources"]}
    assert {"source_id": "retail.orders", "reason": "settle_pending:1"} in p2["no_data"]
    # watermark đã chạm cận trên ⇒ up_to_date
    p3 = plan_ingest(active_contracts(), "2026-01-15", {("wholesale", "orders"): "2026-01-16 00:00:00"}, set(), {},
                     start, "20260115")
    assert {"source_id": "wholesale.orders", "reason": "up_to_date"} in p3["no_data"]
    # file snapshot không có ngày trong tên ⇒ lỗi
    bad = {"retail.customers": [{"name": "customers.csv", "size": 1, "modify_ms": _ms(2026, 1, 10)}]}
    p4 = plan_ingest(active_contracts(), "2026-01-15", {}, set(), bad, start, "20260115")
    assert [e["source_id"] for e in p4["errors"]] == ["retail.customers"]


def _decide(target, activate=True, state=None, versions=None, changed=False, pending=None, **kw):
    spec = get_silver(target) if target.startswith("slv.") else get_gold(target)
    old_active = spec["is_active"]
    saved = {k: globals()[k] for k in ("state_get", "table_version", "data_changed_since", "select_deliveries")}
    spec["is_active"] = activate
    globals().update(state_get=lambda t: state, table_version=lambda t: (versions or {}).get(t, 10),
                     data_changed_since=lambda t, v: changed, select_deliveries=lambda *a, **k: pending or [])
    try:
        return decide_run(target, **kw)
    finally:
        spec["is_active"] = old_active
        globals().update(saved)


def _state(lv=1, age=1, iv=None):
    return {"target_table": "x", "logic_version": lv, "input_versions": iv or {}, "output_version": 5,
            "last_run_id": "r", "last_exec_mode": "full", "age_days": age}


def test_decide__rows():
    g = "gld.dim_customer"
    r = _decide(g, activate=False)
    assert (r["decision"], r["reason"]) == ("SKIP", "inactive"), r
    r = _decide(g, state=_state(), full_reload=True, scope="ALL")
    assert (r["decision"], r["exec_mode"], r["reason"]) == ("RUN", "full", "full_reload"), r
    r = _decide(g, state=_state(), full_reload=True, scope="gld.dim_product")
    assert r["reason"] != "full_reload", "scope không chứa bảng này thì không full_reload"
    r = _decide(g, state=None)
    assert (r["exec_mode"], r["reason"]) == ("full", "first_run"), r
    r = _decide(g, state=_state(lv=99))
    assert (r["exec_mode"], r["reason"]) == ("full", "logic_version"), r
    r = _decide(g, state=_state(age=SETTINGS["cdf_max_age_days"] + 1))
    assert (r["exec_mode"], r["reason"]) == ("full", "checkpoint_too_old"), r
    r = _decide(g, state=_state(), changed=True)
    assert (r["decision"], r["exec_mode"], r["reason"]) == ("RUN", "incremental", "input_changed"), r
    r = _decide(g, state=_state(), changed=False)
    assert (r["decision"], r["reason"]) == ("SKIP", "up_to_date"), r
    s = "slv.orders_event"
    r = _decide(s, state=_state(), pending=[{"delivery_id": "d"}])
    assert (r["decision"], r["exec_mode"], r["reason"]) == ("RUN", "incremental", "pending_deliveries"), r
    r = _decide(s, state=_state())
    assert (r["decision"], r["reason"]) == ("SKIP", "up_to_date"), r


def test_decide__input_versions_shape():
    r = _decide("gld.dim_customer", state=_state(iv={"slv.orders_event": 7}), versions={"slv.orders_event": 9},
                changed=True)
    assert r["input_versions"]["slv.orders_event"] == {"from": 7, "to": 9}, r["input_versions"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- ghi Delta ---------------------------------------------------------------------------------------------

def _t(name, ddl, cdf=False):
    spark.sql(f"DROP TABLE IF EXISTS tst.{name}")
    props = " TBLPROPERTIES (delta.enableChangeDataFeed = true)" if cdf else ""
    spark.sql(f"CREATE TABLE tst.{name} ({ddl}) USING DELTA{props}")
    return f"tst.{name}"


def test_write__align_to_target():
    t = _t("al", "id INT NOT NULL, v STRING, n DECIMAL(18,2)")
    out = align_to_target(t, spark.createDataFrame([("1", "a")], "id string, v string"))
    assert out.columns == ["id", "v", "n"], "thiếu cột nullable ⇒ NULL, đúng thứ tự"
    assert out.collect()[0]["id"] == 1, "phải cast về kiểu bảng"
    for bad, why in ((spark.createDataFrame([(1, "x", None, 5)], "id int, v string, n decimal(18,2), z int"), "cột thừa"),
                      (spark.createDataFrame([("a",)], "v string"), "thiếu NOT NULL")):
        try:
            align_to_target(t, bad)
        except ValueError:
            continue
        raise AssertionError(f"{why}: phải báo lỗi")


def test_write__merge_events():
    t = _t("ev", "_event_key STRING NOT NULL, _record_hash STRING, _delivered_at_utc TIMESTAMP, v STRING")
    d1, d2 = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc), dt.datetime(2026, 1, 2, tzinfo=dt.timezone.utc)
    sch = "_event_key string, _record_hash string, _delivered_at_utc timestamp, v string"

    def put(rows):
        return merge_events(t, spark.createDataFrame(rows, sch))

    m = put([("k1", "h1", d1, "a"), ("k2", "h1", d1, "b")])
    assert m["rows_inserted"] == 2, m
    m = put([("k1", "h1", d1, "a"), ("k2", "h1", d1, "b")])
    assert m["rows_inserted"] == 0 and m["rows_updated"] == 0, f"chạy lại ⇒ 0 thay đổi: {m}"
    m = put([("k1", "h2", d2, "a2")])
    assert m["rows_updated"] == 1, m
    m = put([("k1", "h3", d1, "older")])
    assert m["rows_updated"] == 0, f"delivery cũ hơn phải bị bỏ qua: {m}"
    assert spark.table(t).where("_event_key = 'k1'").collect()[0]["v"] == "a2"
    try:
        put([("k9", "h", d1, "x"), ("k9", "h", d1, "y")])
    except ValueError:
        pass
    else:
        raise AssertionError("input trùng khoá phải báo lỗi")


def test_write__upsert_sync_cdf():
    t = _t("dim", "k INT NOT NULL, h STRING, v STRING", cdf=True)
    sch = "k int, h string, v string"
    m = sync_table(t, spark.createDataFrame([(1, "a", "x"), (2, "a", "y")], sch), ["k"], "h")
    assert m["rows_inserted"] == 2, m
    v0 = table_version(t)
    m = sync_table(t, spark.createDataFrame([(1, "a", "x"), (2, "a", "y")], sch), ["k"], "h")
    assert m["rows_inserted"] == 0 and m["rows_updated"] == 0 and m["rows_deleted"] == 0, m
    ch = read_changes(t, v0, table_version(t))
    assert ch.count() == 0, "hash giống ⇒ CDF rỗng"
    m = sync_table(t, spark.createDataFrame([(1, "b", "x2")], sch), ["k"], "h")
    assert m["rows_updated"] == 1 and m["rows_deleted"] == 1, f"1 sửa, 1 xoá (vắng mặt): {m}"
    assert [r["k"] for r in spark.table(t).collect()] == [1]
    m = merge_upsert(t, spark.createDataFrame([(1, "b", "x2"), (3, "c", "z")], sch), ["k"], "h")
    assert m["rows_inserted"] == 1 and m["rows_updated"] == 0, m
    assert data_changed_since(t, v0) is True
    assert data_changed_since(t, table_version(t)) is False


def test_write__read_changes():
    t = _t("cdf", "k INT NOT NULL, v STRING", cdf=True)
    spark.sql(f"INSERT INTO {t} VALUES (1, 'a')")
    v1 = table_version(t)
    spark.sql(f"INSERT INTO {t} VALUES (2, 'b'), (3, 'c')")
    v2 = table_version(t)
    assert read_changes(t, v2, v2).count() == 0
    assert read_changes(t, v1, v2).count() == 2
    assert sorted(r["k"] for r in changed_keys(t, (v1, v2), ["k"]).collect()) == [2, 3]
    spark.sql(f"OPTIMIZE {t}")
    assert data_changed_since(t, v2) is False, "OPTIMIZE không phải thay đổi dữ liệu"


def test_write__overwrite_table():
    t = _t("ow", "k INT NOT NULL, v STRING", cdf=True)
    m = overwrite_table(t, spark.createDataFrame([(1, "a"), (2, "b")], "k int, v string"))
    assert m["rows_inserted"] == 2, m
    m = overwrite_table(t, spark.createDataFrame([(5, "z")], "k int, v string"))
    assert spark.table(t).count() == 1 and m["rows_inserted"] == 1, m
    try:
        overwrite_table(t, spark.createDataFrame([(None, "z")], "k int, v string"))
    except Exception:  # noqa: BLE001 — DeltaInvariantViolation bọc trong Py4JJavaError
        pass
    else:
        raise AssertionError("NULL vào cột NOT NULL phải lỗi")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- snapshot_changes, quarantine ---------------------------------------------------------------------------

def _snap(rows):
    return spark.createDataFrame(
        [(ss, k, h, False, dt.date.fromisoformat(d)) for ss, k, h, d in rows],
        "source_system string, k string, _record_hash string, is_deleted boolean, _snapshot_date date")


def test_snapshot__changes_full():
    snaps = _snap([("w", "A", "h1", "2026-01-01"), ("w", "B", "h1", "2026-01-01"),
                   ("w", "A", "h1", "2026-01-02"), ("w", "B", "h2", "2026-01-02"),
                   ("w", "B", "h2", "2026-01-03"),
                   ("w", "A", "h1", "2026-01-04"), ("w", "B", "h2", "2026-01-04")])
    out = snapshot_changes(snaps, snaps.limit(0), ["k"])
    got = sorted((r["k"], str(r["_snapshot_date"]), r["_change_kind"], r["is_deleted"]) for r in out.collect())
    want = sorted([("A", "2026-01-01", "INSERT", False), ("B", "2026-01-01", "INSERT", False),
                   ("B", "2026-01-02", "UPDATE", False), ("A", "2026-01-03", "DELETE", True),
                   ("A", "2026-01-04", "REINSERT", False)])
    assert got == want, f"got={got}"


def test_snapshot__changes_incremental():
    prev = spark.createDataFrame(
        [("w", "A", "h1", False, dt.date(2026, 1, 1)), ("w", "B", "h2", False, dt.date(2026, 1, 2))],
        "source_system string, k string, _record_hash string, is_deleted boolean, _snapshot_date date")
    snaps = _snap([("w", "A", "h1", "2026-01-05")])
    out = snapshot_changes(snaps, prev, ["k"])
    got = sorted((r["k"], str(r["_snapshot_date"]), r["_change_kind"]) for r in out.collect())
    assert got == [("B", "2026-01-05", "DELETE")], f"A không đổi, B vắng ⇒ DELETE: {got}"


def _qdf(rows, run_id="run1"):
    """Như dữ liệu conform thật: có cột riêng của lượt (_run_id, _logic_version, _ingested_at_utc) đổi theo lượt."""
    return (spark.createDataFrame(
        rows, "source_system string, _batch_id string, _delivery_id string, _bk string, _reject_reason string, "
              "_dq_flags array<string>, _delivered_at_utc timestamp, _row_no bigint, payload string")
            .withColumn("_run_id", F.lit(run_id)).withColumn("_logic_version", F.lit(1))
            .withColumn("_ingested_at_utc", F.current_timestamp()))


def test_quarantine__reject_flag_resolve():
    t = "tst.quarantine"
    spark.sql(f"DROP TABLE IF EXISTS {t}")
    spark.sql("CREATE TABLE tst.quarantine (entity STRING NOT NULL, source_system STRING NOT NULL, "
              "batch_id STRING NOT NULL, delivery_id STRING NOT NULL, business_key STRING, reason STRING NOT NULL, "
              "action STRING NOT NULL, row_json STRING NOT NULL, run_id STRING NOT NULL, "
              "quarantined_at_utc TIMESTAMP NOT NULL, resolved_at_utc TIMESTAMP, resolved_run_id STRING) USING DELTA")
    d1 = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    rows = [("r", "20260101", "dl1", "r|O1|P1", "invalid_price", [], d1, 1, "x"),
            ("r", "20260101", "dl1", None, "missing_business_key", [], d1, 2, "y"),
            ("r", "20260101", "dl1", "r|O2|P1", None, ["negative_quantity"], d1, 3, "z"),
            ("r", "20260101", "dl1", "r|O3|P1", None, [], d1, 4, "ok")]
    n_rej, n_flag = quarantine_rows(_qdf(rows), "orders", "run1", table=t)
    assert (n_rej, n_flag) == (2, 1), (n_rej, n_flag)
    quarantine_rows(_qdf(rows, "run2"), "orders", "run2", table=t)
    assert spark.table(t).count() == 3, "xử lý lại ở lượt khác không được ghi trùng (kể cả khoá NULL)"
    assert not any(c in r["row_json"] for r in spark.table(t).collect() for c in ("_run_id", "_ingested_at_utc")), \
        "row_json không được chứa cột riêng của lượt"
    resolve_quarantine("dl2", "run3", table=t)
    assert spark.table(t).where("resolved_at_utc IS NULL AND action = 'REJECT'").count() == 2, \
        "sự kiện hợp lệ ở delivery khác không được tự đóng REJECT của delivery dl1"
    resolve_quarantine("dl1", "run4", table=t)
    assert spark.table(t).where("resolved_at_utc IS NULL AND action = 'REJECT'").count() == 0
    assert spark.table(t).where("action = 'FLAG' AND resolved_at_utc IS NULL").count() == 1, "FLAG không bị đóng"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- latest_event, hash_columns -----------------------------------------------------------------------------

def test_current__latest_event():
    ts = dt.datetime(2026, 1, 1, 10, tzinfo=dt.timezone.utc)
    d = dt.datetime(2026, 1, 2, tzinfo=dt.timezone.utc)
    ev = spark.createDataFrame(
        [("r", "O1", "P1", ts, 3, d, 1, "Shipped"), ("r", "O1", "P1", ts, 4, d, 2, "Delivered"),
         ("r", "O1", "P1", ts - dt.timedelta(hours=1), 6, d, 3, "Cancelled"),
         ("r", "O2", "P1", ts, 1, d, 4, "Pending")],
        "source_system string, order_no string, product_code string, updated_at_utc timestamp, status_rank int, "
        "_delivered_at_utc timestamp, _row_no bigint, order_status string")
    got = {(r["order_no"], r["order_status"]) for r in latest_event(ev).collect()}
    assert got == {("O1", "Delivered"), ("O2", "Pending")}, got


def test_config__hash_columns():
    cols = hash_columns(get_silver("slv.orders_event"))
    assert "updated_at_utc" in cols and "created_at_utc" not in cols
    assert "order_status_raw" not in cols and "tax_rate_raw" not in cols

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
    except Exception as exc:  # noqa: BLE001
        results.append((name, "FAIL", str(exc)[:500]))
spark.sql("DROP SCHEMA IF EXISTS tst CASCADE")
display(spark.createDataFrame(results, "test string, status string, error string"))
failed = [r[0] for r in results if r[1] == "FAIL"]
if failed:
    raise AssertionError(f"{len(failed)} test(s) failed: {failed}")
print(f"{len(results)} tests PASS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

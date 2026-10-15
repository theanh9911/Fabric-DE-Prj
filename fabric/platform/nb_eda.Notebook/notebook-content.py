# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_eda (T0.3 — tạm, xoá ở T8.4)
# Khảo sát nguồn **trực tiếp**, chỉ đọc: file CSV ở drop zone và bảng ERP (Delta của item SQL Database trên OneLake).
# Không cần `lh_platform`, không cần dữ liệu platform nào. Run all rồi dán toàn bộ output cho người review.
#
# Phụ thuộc: nguồn đã có dữ liệu (đã chạy `nb_00_sim_daily` initial load) và identity chạy có quyền đọc workspace Source.
# Checklist: docs/EDA.md (E01–E20).
#
# Mỗi file CSV được đọc riêng rồi `unionByName` (header có thể khác nhau giữa các file; đọc gộp sẽ áp header của file đầu cho mọi file).

# PARAMETERS CELL ********************

p_source_workspace = "CompanyA-Source"
p_drop_lakehouse = "lh_retail_drop"
p_erp_item = "sqldb_erp_wholesale"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sempy.fabric as fabric
from pyspark.sql import functions as F

NULL_LIKE = ["", "NULL", "N/A", "NA", "NONE", "NIL", "-"]

WS = fabric.resolve_workspace_id(p_source_workspace)
DROP = fabric.resolve_item_id(p_drop_lakehouse, "Lakehouse", WS)
ERP = fabric.resolve_item_id(p_erp_item, "SQLDatabase", WS)
ONELAKE = f"abfss://{WS}@onelake.dfs.fabric.microsoft.com"
print("workspace", WS, "| drop", DROP, "| erp", ERP)


def landing_files(source, entity, ext=".csv"):
    if source == "wholesale":
        return []
    d = f"{ONELAKE}/{DROP}/Files/inbound/{source}/{entity}"
    if not notebookutils.fs.exists(d):
        return []
    return sorted(f.path for f in notebookutils.fs.ls(d) if f.name.endswith(".csv"))


def load(source, entity, fmt=None):
    if source == "wholesale":
        df = spark.read.format("delta").load(f"{ONELAKE}/{ERP}/Tables/dbo/{entity}")
        df = df.select([F.col(c).cast("string").alias(c.strip().lower()) for c in df.columns])
        return df.withColumn("_file", F.lit("erp"))
    paths = landing_files(source, entity)
    if not paths:
        return None
    out = None
    for p in paths:
        d = (spark.read.option("header", True).option("multiLine", True).option("escape", '"')
             .option("inferSchema", False).csv(p))
        d = d.select([F.col(c).cast("string").alias(c.strip().lower()) for c in d.columns])
        d = d.withColumn("_file", F.lit(p.rsplit("/", 1)[-1]))
        out = d if out is None else out.unionByName(d, allowMissingColumns=True)
    return out


def has(df, c):
    return df is not None and c in df.columns


def col(df, c):
    return F.trim(F.col(c)) if has(df, c) else F.lit(None).cast("string")


def shape(e):
    return F.regexp_replace(F.regexp_replace(e, "[0-9]", "9"), "[A-Za-z]", "a")


def freq(title, df, expr, n=30):
    print(f"\n--- {title}")
    df.groupBy(expr.alias("v")).count().orderBy(F.desc("count")).show(n, truncate=70)


def latest(df):
    m = df.agg(F.max("_file")).collect()[0][0]
    return df.filter(F.col("_file") == m)


def section(name):
    print(f"\n{'=' * 24} {name} {'=' * 24}")


def guard(fn):
    try:
        fn()
    except Exception as e:
        print(f"!! lỗi: {type(e).__name__}: {str(e)[:400]}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

D = {}
for s in ("retail", "wholesale"):
    for e in ("orders", "customers", "products", "sales_hierarchy"):
        try:
            D[(s, e)] = load(s, e)
        except Exception as ex:
            print(f"!! không đọc được {(s, e)}: {type(ex).__name__}: {str(ex)[:300]}")
            D[(s, e)] = None
D[("reference", "categories")] = load("reference", "categories")

for k, v in D.items():
    if v is not None:
        v.cache()
    print(k, "KHÔNG CÓ DỮ LIỆU" if v is None else f"{v.count()} dòng, {max(len(landing_files(k[0], k[1])), 1)} file")
ro, wo = D[("retail", "orders")], D[("wholesale", "orders")]
ORDERS = [("retail", ro), ("wholesale", wo)]
MASTERS = [(k, v) for k, v in D.items() if k[1] != "orders" and v is not None]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

section("E01 tên cột theo file")


def e01():
    for (s, e), df in D.items():
        if df is None:
            continue
        if s == "wholesale":
            print((s, e), df.columns)
            continue
        files = landing_files(s, e, ".csv")
        heads = {}
        for p in files:
            h = notebookutils.fs.head(p, 4000).split("\n", 1)[0].strip().lstrip("﻿")
            heads.setdefault(h, []).append(p.rsplit("/", 1)[-1])
        print((s, e), f"{len(files)} file, {len(heads)} header khác nhau")
        for h, fs in heads.items():
            print("   ", h, "<=", len(fs), "file, ví dụ", fs[0])


guard(e01)

section("E02 định dạng thời gian")


def e02():
    for s, df in ORDERS:
        for c in ("created_at", "inserted_at", "updated_at"):
            if has(df, c):
                freq(f"{s}.orders.{c} (hình dạng)", df, shape(col(df, c)), 15)


guard(e02)


def dm_class(e):
    a = F.regexp_extract(e, r"^(\d{1,2})/(\d{1,2})/(\d{4})", 1).cast("int")
    b = F.regexp_extract(e, r"^(\d{1,2})/(\d{1,2})/(\d{4})", 2).cast("int")
    return (F.when(a.isNull() | b.isNull(), "not_slash").when(a > 12, "dmy_sure")
            .when(b > 12, "mdy_sure").when(a == b, "same").otherwise("ambiguous"))


section("E03 định dạng order_date, số dòng mơ hồ")


def e03():
    for s, df in ORDERS:
        if has(df, "order_date"):
            freq(f"{s}.orders.order_date (hình dạng)", df, shape(col(df, "order_date")), 15)
            freq(f"{s}.orders.order_date (lớp d/M vs M/d)", df, dm_class(col(df, "order_date")), 10)
        if has(df, "updated_at"):
            freq(f"{s}.orders.updated_at (lớp d/M vs M/d)", df, dm_class(col(df, "updated_at")), 10)


guard(e03)

section("E04 file trộn hai thứ tự ngày")


def e04():
    for s, df in ORDERS:
        for c in ("order_date", "updated_at"):
            if not has(df, c):
                continue
            k = dm_class(col(df, c))
            r = (df.groupBy("_file").agg(F.sum((k == "dmy_sure").cast("int")).alias("dmy_sure"),
                                         F.sum((k == "mdy_sure").cast("int")).alias("mdy_sure"),
                                         F.sum((k == "ambiguous").cast("int")).alias("ambiguous")))
            print(f"\n--- {s}.orders.{c}: file có cả d/M chắc chắn và M/d chắc chắn")
            r.where((F.col("dmy_sure") > 0) & (F.col("mdy_sure") > 0)).orderBy("_file").show(30, truncate=70)
            print(f"--- {s}.orders.{c}: tổng theo lớp")
            r.agg(F.sum("dmy_sure"), F.sum("mdy_sure"), F.sum("ambiguous")).show()


guard(e04)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

section("E05 múi giờ ERP: phân bố giờ")


def e05():
    for c in ("created_at", "inserted_at", "updated_at"):
        if has(wo, c):
            print(f"\n--- wholesale.orders.{c}: số dòng theo giờ trong ngày")
            (wo.groupBy(F.hour(F.col(c).cast("timestamp")).alias("hour")).count().orderBy("hour").show(24))
    print("\n--- khoảng updated_at / created_at của wholesale")
    wo.agg(F.min(F.col("updated_at").cast("timestamp")), F.max(F.col("updated_at").cast("timestamp")),
           F.min(F.col("created_at").cast("timestamp")), F.max(F.col("created_at").cast("timestamp"))).show(truncate=False)


guard(e05)

section("E06 biến thể order_status")


def e06():
    for s, df in ORDERS:
        freq(f"{s}.orders.order_status (trim)", df, col(df, "order_status"), 40)


guard(e06)

section("E07 hai trạng thái khác nhau cùng (order_no, product_code, updated_at)")


def e07():
    for s, df in ORDERS:
        g = (df.groupBy(col(df, "order_no").alias("o"), col(df, "product_code").alias("p"),
                        col(df, "updated_at").alias("u"))
             .agg(F.countDistinct(F.upper(col(df, "order_status"))).alias("n_status"),
                  F.collect_set(col(df, "order_status")).alias("statuses"))
             .where(F.col("n_status") > 1))
        print(f"\n--- {s}: {g.count()} khoá có >1 trạng thái")
        g.show(15, truncate=70)


guard(e07)

section("E08 event key trùng; nội dung có khác không")


def e08():
    for s, df in ORDERS:
        cols = [c for c in df.columns if c != "_file"]
        h = F.sha2(F.concat_ws("\x1f", *[F.coalesce(F.col(c), F.lit("<NULL>")) for c in cols]), 256)
        g = (df.groupBy(col(df, "order_no").alias("o"), col(df, "product_code").alias("p"),
                        F.upper(col(df, "order_status")).alias("st"), col(df, "updated_at").alias("u"))
             .agg(F.count("*").alias("n"), F.countDistinct(h).alias("n_content"))
             .where(F.col("n") > 1))
        print(f"\n--- {s}: nhóm trùng event key = {g.count()}, trong đó nội dung khác nhau = "
              f"{g.where(F.col('n_content') > 1).count()}")
        g.where(F.col("n_content") > 1).show(10, truncate=70)


guard(e08)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

section("E09 order_no có ở cả hai nguồn không")


def e09():
    a = ro.select(col(ro, "order_no").alias("o")).distinct()
    b = wo.select(col(wo, "order_no").alias("o")).distinct()
    print("retail:", a.count(), "wholesale:", b.count(), "chung:", a.join(b, "o").count())
    a.join(b, "o").show(10)


guard(e09)

section("E10 biến thể tax_rate")


def e10():
    for s, df in ORDERS:
        freq(f"{s}.orders.tax_rate", df, col(df, "tax_rate"), 40)


guard(e10)


def num_class(e):
    return (F.when(e.isNull() | (e == ""), "null_blank").when(e.rlike(r"^-?\d+$"), "int")
            .when(e.rlike(r"^-?\d+\.\d+$"), "decimal").when(e.rlike(r"^-?[\d,]+(\.\d+)?$"), "has_comma")
            .otherwise("other"))


section("E11 quantity, price")


def e11():
    for s, df in ORDERS:
        for c in ("quantity", "price"):
            e = col(df, c)
            freq(f"{s}.orders.{c} (loại)", df, num_class(e), 10)
            print(f"   âm: {df.where(e.startswith('-')).count()}")
            print("   mẫu 'other':", [r.v for r in df.where(num_class(e) == 'other').select(e.alias('v')).limit(8).collect()])


guard(e11)

section("E12 order_date xa trong tương lai")


def e12():
    for s, df in ORDERS:
        y = F.regexp_extract(col(df, "order_date"), r"(20\d{2})", 1)
        freq(f"{s}.orders.order_date (năm)", df, y, 20)


guard(e12)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

section("E13 biến thể thuộc tính master")


def e13():
    for (s, e), df in MASTERS:
        for c in ("gender", "position", "brand", "country", "city", "division"):
            if has(df, c):
                freq(f"{s}.{e}.{c}", df, col(df, c), 25)


guard(e13)

section("E14 giá trị NULL / rỗng / chữ N/A theo cột")


def e14():
    for k, df in D.items():
        if df is None:
            continue
        aggs = []
        for c in df.columns:
            if c == "_file":
                continue
            t = F.upper(F.trim(F.col(c)))
            aggs += [F.sum(F.col(c).isNull().cast("int")).alias(f"{c}__null"),
                     F.sum((t == "").cast("int")).alias(f"{c}__blank"),
                     F.sum(t.isin([x for x in NULL_LIKE if x]).cast("int")).alias(f"{c}__text")]
        r = df.agg(*aggs).collect()[0].asDict()
        bad = {k2: v for k2, v in r.items() if v}
        print(k, bad if bad else "không có")


guard(e14)

section("E15 trùng khoá trong cùng snapshot")
KEYS = {"customers": "customer_code", "products": "product_code", "sales_hierarchy": "salesman_code",
        "categories": "category_code"}


def e15():
    for (s, e), df in MASTERS:
        kc = KEYS[e]
        if not has(df, kc):
            print((s, e), f"thiếu cột {kc}")
            continue
        g = (df.groupBy("_file", F.upper(col(df, kc)).alias("k")).count().where(F.col("count") > 1))
        print(f"\n--- {s}.{e}: {g.count()} (file, khoá) trùng")
        g.orderBy(F.desc("count")).show(10, truncate=70)


guard(e15)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

section("E16 sales_hierarchy: tự trỏ, vòng, quản lý không tồn tại")


def e16():
    for s in ("retail", "wholesale"):
        df = D[(s, "sales_hierarchy")]
        if df is None or not has(df, "salesmanager_code"):
            print(s, "không có dữ liệu hoặc thiếu salesmanager_code")
            continue
        snap = latest(df)
        code, mgr = F.upper(col(snap, "salesman_code")), F.upper(col(snap, "salesmanager_code"))
        print(f"\n--- {s} (snapshot mới nhất)")
        print("tự trỏ:", snap.where(code == mgr).count())
        codes = snap.select(code.alias("c")).distinct()
        print("quản lý không tồn tại:", snap.where(mgr.isNotNull() & (mgr != "")).select(mgr.alias("m"))
              .distinct().join(codes, F.col("m") == F.col("c"), "left_anti").count())
        e = (snap.select(code.alias("s"), mgr.alias("m")).where(F.col("m").isNotNull() & (F.col("m") != ""))
             .distinct())
        cur = e.withColumnRenamed("s", "a").withColumnRenamed("m", "b")
        step = e.withColumnRenamed("s", "b").withColumnRenamed("m", "c")
        for i in range(2, 9):
            cur = cur.join(step, "b").select("a", F.col("c").alias("b")).distinct()
            print(f"vòng độ dài {i}:", cur.where(F.col("a") == F.col("b")).count())


guard(e16)

section("E17 cùng khoá ở hai nguồn: lệch cột nào")


def e17():
    for e in ("customers", "products", "sales_hierarchy"):
        r, w = D[("retail", e)], D[("wholesale", e)]
        if r is None or w is None:
            print(e, "thiếu một nguồn")
            continue
        kc = KEYS[e]
        a, b = latest(r), latest(w)
        shared = [c for c in a.columns if c in b.columns and c not in ("_file", kc)]
        j = a.alias("a").join(b.alias("b"), F.upper(F.trim(F.col(f"a.{kc}"))) == F.upper(F.trim(F.col(f"b.{kc}"))))
        print(f"\n--- {e}: khoá chung = {j.count()}, cột chung = {shared}")
        if shared:
            aggs = [F.sum((F.trim(F.col(f"a.{c}")) != F.trim(F.col(f"b.{c}"))).cast("int")).alias(c) for c in shared]
            j.agg(*aggs).show(truncate=False)


guard(e17)

section("E18 mã mồ côi trong orders")


def e18():
    def codes(e, c):
        out = None
        for (s, e2), df in MASTERS:
            if e2 == e and has(df, c):
                p = df.select(F.upper(col(df, c)).alias("k"))
                out = p if out is None else out.union(p)
        return out.distinct() if out is not None else None

    for s, df in ORDERS:
        for c, me in (("customer_code", "customers"), ("product_code", "products"),
                      ("salesman_code", "sales_hierarchy")):
            if not has(df, c):
                continue
            ref = codes(me, KEYS[me])
            used = df.select(F.upper(col(df, c)).alias("k"), "_file").where(F.col("k").isNotNull() & (F.col("k") != ""))
            if ref is None:
                print(s, c, "không có master để so")
                continue
            orphan = used.join(ref, "k", "left_anti")
            print(f"\n--- {s}.orders.{c}: dòng mồ côi = {orphan.count()}, mã mồ côi khác nhau = "
                  f"{orphan.select('k').distinct().count()}")
            orphan.groupBy("k").count().orderBy(F.desc("count")).show(10)


guard(e18)

section("E19 category_code của products không có trong categories")


def e19():
    cat = D[("reference", "categories")]
    if cat is None:
        print("không có categories")
        return
    ref = cat.select(F.upper(col(cat, "category_code")).alias("k")).distinct()
    for s in ("retail", "wholesale"):
        p = D[(s, "products")]
        if not has(p, "category_code"):
            print(s, "products thiếu category_code")
            continue
        used = p.select(F.upper(col(p, "category_code")).alias("k")).where(F.col("k").isNotNull()).distinct()
        o = used.join(ref, "k", "left_anti")
        print(f"\n--- {s}: {o.count()} category_code không có trong categories")
        o.show(20)


guard(e19)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

section("E20 số sự kiện mỗi dòng hàng; trạng thái đi lùi")
RANK = {"PENDING": 1, "CONFIRMED": 2, "SHIPPED": 3, "DELIVERED": 4, "DELIVRED": 4, "CANCELLED": 5}


def e20():
    from pyspark.sql import Window
    for s, df in ORDERS:
        o, p = col(df, "order_no"), col(df, "product_code")
        ts = F.expr("coalesce(try_to_timestamp(trim(updated_at)), try_to_timestamp(trim(updated_at), 'M/d/yyyy H:mm'),"
                    " try_to_timestamp(trim(updated_at), 'd/M/yyyy H:mm'))")
        key = F.regexp_replace(F.upper(col(df, "order_status")), "[^A-Z]", "")
        rank = F.create_map(*[x for kv in RANK.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])[key]
        base = df.select(o.alias("o"), p.alias("p"), ts.alias("ts"), rank.alias("rk"))
        print(f"\n--- {s}: updated_at không parse được = {base.where(F.col('ts').isNull()).count()}")
        n = base.groupBy("o", "p").count()
        print("số sự kiện mỗi dòng hàng:")
        n.groupBy("count").count().orderBy("count").show(15)
        w = Window.partitionBy("o", "p").orderBy("ts")
        back = base.where(F.col("ts").isNotNull()).withColumn("prev", F.lag("rk").over(w)) \
            .where(F.col("prev").isNotNull() & F.col("rk").isNotNull() & (F.col("rk") < F.col("prev")))
        print("dòng hàng có bước lùi trạng thái:", back.select("o", "p").distinct().count())
        back.show(10, truncate=40)


guard(e20)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

# # nb_config
# Cấu hình platform dưới dạng code: `SETTINGS`, từ điển, `SILVER`, `GOLD`, `CHECKS`, `validate_config()`.
# Không đọc/ghi dữ liệu. Spec: docs/05-thu-vien.md §2. Đổi một object ⇒ tăng `logic_version` của nó.

# CELL ********************

%run nb_contracts

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SETTINGS = {
    "business_timezone": "Asia/Ho_Chi_Minh",
    "date_dim_start": "2020-01-01",
    "date_dim_end": "2035-12-31",
    "future_order_days": 1,
    "source_priority": ["wholesale", "retail"],
    "full_checks_weekday": 6,
    "cdf_max_age_days": 25,
}

# Khoá của mọi từ điển = dict_key(giá trị thô): chữ hoa, bỏ ký tự không phải chữ. Nguồn: EDA E06, E13.
ORDER_STATUS = {"PENDING": "Pending", "PENDNG": "Pending", "CONFIRMED": "Confirmed", "CONFIRMD": "Confirmed",
                "SHIPPED": "Shipped", "SHIPPD": "Shipped", "DELIVERED": "Delivered", "DELIVERD": "Delivered",
                "RETURNED": "Returned", "RETURN": "Returned", "CANCELLED": "Cancelled", "CANCEL": "Cancelled"}
STATUS_RANK = {"Pending": 1, "Confirmed": 2, "Shipped": 3, "Delivered": 4, "Returned": 5, "Cancelled": 6}
TAX_WORDS = {"FIVEPERCENT": "0.05"}
GENDER = {"F": "Female", "FEMALE": "Female", "M": "Male", "MALE": "Male", "MAEL": "Male", "UNKNOWN": "Unknown",
POSITION = {"SALESMAN": "Salesman", "SALE": "Salesman", "SALSEMAN": "Salesman", "TEAMLEAD": "Team Lead",
            "MANAGER": "Manager", "DIRECTOR": "Director"}
BRAND = {"IKEA": "IKEA", "APPLE": "Apple", "ZARA": "Zara", "UNIQLO": "Uniqlo", "HP": "HP", "LENOVO": "Lenovo",
         "SAMSUNG": "Samsung", "HM": "H&M", "DELL": "Dell", "ACER": "Acer", "TRUNGNGUYEN": "Trung Nguyen",
         "RAZER": "Razer", "OPPO": "OPPO", "XIAOMI": "Xiaomi", "ASUS": "Asus", "MAXWELLHOUSE": "Maxwell House",
         "HIGHLANDSCOFFEE": "Highlands Coffee", "LEVIS": "Levi's", "STARBUCKS": "Starbucks",
         "MICROSOFT": "Microsoft", "NESTLE": "Nestle", "NESCAFE": "Nescafe", "VIVO": "Vivo", "MSI": "MSI"}
DICTIONARIES = dict(ORDER_STATUS=ORDER_STATUS, STATUS_RANK=STATUS_RANK, TAX_WORDS=TAX_WORDS,
                    GENDER=GENDER, POSITION=POSITION, BRAND=BRAND)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

_HISTORY_TECH_HASH_EXCLUDE = ["created_at_utc", "updated_at_utc"]

SILVER = [
    dict(table="slv.orders_event", entity="orders", kind="event", notebook="nb_slv_orders",
         logic_version=1, is_active=True,
         business_keys=["order_no", "product_code"],
         event_keys=["order_no", "product_code", "order_status", "updated_at_utc"],
         hash_exclude=["order_status_raw", "tax_rate_raw", "created_at_utc"],
         columns=[
             dict(name="order_no", type="string", std="code", required=True),
             dict(name="product_code", type="string", std="code", required=True),
             dict(name="customer_code", type="string", std="code"),
             dict(name="salesman_code", type="string", std="code"),
             dict(name="order_date", type="date", std="date", required=True, reject_reason="invalid_order_date"),
             dict(name="order_status", type="string", std="dict:ORDER_STATUS", required=True,
                  keep_raw="order_status_raw", reject_reason="unknown_order_status"),
             dict(name="status_rank", type="int", std="lookup:STATUS_RANK", from_column="order_status"),
             dict(name="quantity", type="decimal(18,2)", std="decimal", required=True,
                  reject_reason="invalid_quantity", flag_negative="negative_quantity"),
             dict(name="price", type="decimal(18,2)", std="decimal", required=True,
                  reject_reason="invalid_price", flag_negative="negative_price"),
             dict(name="tax_rate", type="decimal(9,6)", std="tax_rate", keep_raw="tax_rate_raw",
                  flag_reason="invalid_tax_rate"),
             dict(name="created_at_utc", type="timestamp", std="ts", source="created_at"),
             dict(name="updated_at_utc", type="timestamp", std="ts", source="updated_at", required=True,
                  reject_reason="invalid_updated_at"),
         ]),
    dict(table="slv.customers_history", entity="customers", kind="snapshot", notebook="nb_slv_master",
         logic_version=2, is_active=True,
         business_keys=["customer_code"],
         hash_exclude=_HISTORY_TECH_HASH_EXCLUDE + ["gender_raw"],
         columns=[
             dict(name="customer_code", type="string", std="code", required=True),
             dict(name="customer_name", type="string", std="text"),
             dict(name="country", type="string", std="title"),
             dict(name="city", type="string", std="title"),
             dict(name="gender", type="string", std="dict:GENDER", keep_raw="gender_raw"),
             dict(name="address", type="string", std="text"),
             dict(name="created_at_utc", type="timestamp", std="ts", source="created_at"),
             dict(name="updated_at_utc", type="timestamp", std="ts", source="updated_at"),
         ]),
    dict(table="slv.products_history", entity="products", kind="snapshot", notebook="nb_slv_master",
         logic_version=1, is_active=True,
         business_keys=["product_code"],
         hash_exclude=_HISTORY_TECH_HASH_EXCLUDE + ["brand_raw"],
         columns=[
             dict(name="product_code", type="string", std="code", required=True),
             dict(name="product_name", type="string", std="text"),
             dict(name="brand", type="string", std="dict:BRAND", keep_raw="brand_raw"),
             dict(name="category_code", type="string", std="code"),
             dict(name="created_at_utc", type="timestamp", std="ts", source="created_at"),
             dict(name="updated_at_utc", type="timestamp", std="ts", source="updated_at"),
         ]),
    dict(table="slv.sales_hierarchy_history", entity="sales_hierarchy", kind="snapshot",
         notebook="nb_slv_master", logic_version=1, is_active=True,
         business_keys=["salesman_code"],
         hash_exclude=_HISTORY_TECH_HASH_EXCLUDE + ["position_raw"],
         columns=[
             dict(name="salesman_code", type="string", std="code", required=True),
             dict(name="salesman_name", type="string", std="text"),
             dict(name="division", type="string", std="title"),
             dict(name="position", type="string", std="dict:POSITION", keep_raw="position_raw"),
             dict(name="salesmanager_code", type="string", std="code"),
             dict(name="salesmanager_name", type="string", std="text"),
             dict(name="created_at_utc", type="timestamp", std="ts", source="inserted_at"),
             dict(name="updated_at_utc", type="timestamp", std="ts", source="updated_at"),
         ]),
    dict(table="slv.categories_history", entity="categories", kind="snapshot", notebook="nb_slv_master",
         logic_version=1, is_active=True,
         business_keys=["category_code"],
         hash_exclude=[],
         columns=[
             dict(name="category_code", type="string", std="code", required=True),
             dict(name="category_lvl1", type="string", std="text"),
             dict(name="category_lvl2", type="string", std="text"),
             dict(name="category_lvl3", type="string", std="text"),
             dict(name="category_lvl4", type="string", std="text"),
         ]),
]

GOLD = [
    dict(table="gld.dim_date", notebook="nb_gld_dim_date", logic_version=1, is_active=True,
         inputs=[], grain=["date_key"], rules=[]),
    dict(table="gld.dim_customer", notebook="nb_gld_dim_customer", logic_version=1, is_active=True,
         inputs=[dict(table="slv.customers_history", read="full"), dict(table="slv.orders_event", read="cdf")],
         grain=["customer_code"], rules=["R-CUS-01", "R-CUS-02", "R-INF-01"]),
    dict(table="gld.dim_product", notebook="nb_gld_dim_product", logic_version=1, is_active=True,
         inputs=[dict(table="slv.products_history", read="full"),
                 dict(table="slv.categories_history", read="full"),
                 dict(table="slv.orders_event", read="cdf")],
         grain=["product_code"], rules=["R-PRD-01", "R-PRD-02", "R-INF-01"]),
    dict(table="gld.dim_salesman", notebook="nb_gld_dim_salesman", logic_version=1, is_active=True,
         inputs=[dict(table="slv.sales_hierarchy_history", read="full"), dict(table="slv.orders_event", read="cdf")],
         grain=["source_system", "salesman_code", "valid_from"],
         rules=["R-SLM-01", "R-SLM-02", "R-SLM-03", "R-SLM-04"]),
    dict(table="gld.fct_order_line", notebook="nb_gld_fct_order_line", logic_version=1, is_active=True,
         inputs=[dict(table="slv.orders_event", read="cdf"), dict(table="gld.dim_salesman", read="cdf"),
                 dict(table="gld.dim_customer", read="dep"), dict(table="gld.dim_product", read="dep"),
                 dict(table="gld.dim_date", read="dep")],
         grain=["source_system", "order_no", "product_code"], rules=["R-ORD-04", "R-SLM-05"]),
]

# Danh mục check nền tảng (docs/04-kien-truc.md §13). DQ-OPS-01 do nb_sla_watch chạy.
def _chk(check_id, obj, type_, severity, scope, params=None, threshold=None, description=None):
    c = dict(check_id=check_id, object=obj, type=type_, severity=severity, scope=scope)
    if params:
        c["params"] = params
    if threshold is not None:
        c["threshold"] = threshold
    if description:
        c["description"] = description
    return c


_HISTORY_KEYS = {"slv.customers_history": "customer_code", "slv.products_history": "product_code",
                 "slv.sales_hierarchy_history": "salesman_code", "slv.categories_history": "category_code"}
_DIMS = {"gld.dim_customer": "customer_sk", "gld.dim_product": "product_sk",
         "gld.dim_salesman": "salesman_sk", "gld.dim_date": "date_key"}

CHECKS = (
    [_chk("DQ-SLV-01", "slv.orders_event", "unique", "critical", "touched", {"cols": ["_event_key"]}),
     _chk("DQ-SLV-02", "slv.orders_event", "not_null", "critical", "touched",
          {"cols": ["order_no", "product_code", "order_status", "order_date", "updated_at_utc"]})]
    + [_chk(f"DQ-SLV-03.{t}", t, "unique", "critical", "touched", {"cols": ["source_system", k, "_snapshot_date"]})
       for t, k in _HISTORY_KEYS.items()]
    + [_chk("DQ-SLV-04", "meta.delivery_log", "quarantine_rate", "critical", "run"),
       _chk("DQ-SLV-05", "slv.orders_event", "flag_count", "warning", "touched",
            {"flags": ["ambiguous_date", "negative_quantity", "negative_price"]}),
       _chk("DQ-SLV-06", "meta.delivery_log", "schema_drift", "warning", "run"),
       _chk("DQ-SLV-07", "slv.orders_event", "event_date_vs_file", "warning", "touched")]
    + [_chk(f"DQ-GLD-01.{t}", t, "unique", "critical", "full", {"cols": [sk]}) for t, sk in _DIMS.items()]
    + [_chk("DQ-GLD-02.gld.dim_customer", "gld.dim_customer", "unique", "critical", "full",
            {"cols": ["customer_code"]}),
       _chk("DQ-GLD-02.gld.dim_product", "gld.dim_product", "unique", "critical", "full",
            {"cols": ["product_code"]}),
       _chk("DQ-GLD-03", "gld.dim_salesman", "scd2_one_current", "critical", "full"),
       _chk("DQ-GLD-04", "gld.dim_salesman", "scd2_contiguous", "critical", "full"),
       _chk("DQ-GLD-05", "gld.fct_order_line", "unique", "critical", "touched",
            {"cols": ["source_system", "order_no", "product_code"]}),
       _chk("DQ-GLD-06", "gld.fct_order_line", "not_null", "critical", "touched",
            {"cols": ["customer_sk", "product_sk", "salesman_sk", "order_date_key", "current_status", "quantity",
                      "price"]}),
       _chk("DQ-GLD-07", "gld.fct_order_line", "unknown_member_rate", "warning", "touched", threshold=1)]
    + [_chk(f"DQ-GLD-08.{t}", t, "sk_collision", "critical", "full",
            {"sk": sk, "natural": {"gld.dim_customer": ["customer_code"], "gld.dim_product": ["product_code"],
                                   "gld.dim_salesman": ["source_system", "salesman_code", "valid_from"],
                                   "gld.dim_date": ["date_value"]}[t]})
       for t, sk in _DIMS.items()]
    + [_chk("DQ-ING-01", "meta.landed_file", "duplicate_file", "warning", "run"),
       _chk("DQ-OPS-01", "meta.delivery_log", "freshness", "warning", "run"),
       _chk("DQ-OPS-03", "meta.delivery_log", "parked_delivery", "warning", "run")]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

STD_PLAIN = {"code", "text", "title", "ts", "date", "decimal", "tax_rate"}
CHECK_TYPES = {"unique", "not_null", "quarantine_rate", "flag_count", "schema_drift", "scd2_one_current",
               "scd2_contiguous", "unknown_member_rate", "sk_collision", "duplicate_file", "freshness",
               "event_date_vs_file", "parked_delivery"}
SETTINGS_KEYS = {"business_timezone", "date_dim_start", "date_dim_end", "future_order_days", "source_priority",
                 "full_checks_weekday", "cdf_max_age_days"}
META_TABLES = {"meta.delivery_log", "meta.landed_file", "meta.run_log", "meta.check_result", "meta.table_state"}


def _is_pos_int(v):
    return isinstance(v, int) and not isinstance(v, bool) and v >= 1


def validate_config():
    """Kiểm V01–V10 (docs/05 §2.6); lỗi ⇒ ValueError liệt kê mọi vi phạm."""
    errors = []

    def err(code, msg):
        errors.append(f"{code}: {msg}")

    objects = SILVER + GOLD
    tables = [o["table"] for o in objects]
    silver_by_table = {s["table"]: s for s in SILVER}

    # V01
    for t in sorted({t for t in tables if tables.count(t) > 1}):
        err("V01", f"{t} khai báo nhiều lần")

    # V02
    live = [c for c in CONTRACTS.values() if c["status"] != "retired"]
    for c in live:
        s = silver_by_table.get(c["silver_table"])
        if s is None:
            err("V02", f"{c['id']}: silver_table {c['silver_table']} không có trong SILVER")
        elif s["kind"] != c["kind"]:
            err("V02", f"{c['id']}: kind {c['kind']} khác kind {s['kind']} của {s['table']}")
    fed = {c["silver_table"] for c in live}
    for s in SILVER:
        if s["table"] not in fed:
            err("V02", f"{s['table']} không có contract nào")

    # V03, V04, V05 (Silver)
    for s in SILVER:
        names = [c["name"] for c in s["columns"]]
        raws = [c["keep_raw"] for c in s["columns"] if c.get("keep_raw")]
        for n in {n for n in names if names.count(n) > 1}:
            err("V05", f"{s['table']}: cột {n} trùng")
        feeding = [c for c in live if c["silver_table"] == s["table"]]
        for col in s["columns"]:
            std = col["std"]
            if std.startswith("dict:") or std.startswith("lookup:"):
                kind, _, dname = std.partition(":")
                if dname not in DICTIONARIES:
                    err("V03", f"{s['table']}.{col['name']}: từ điển {dname} không tồn tại")
            elif std not in STD_PLAIN:
                err("V03", f"{s['table']}.{col['name']}: std '{std}' không thuộc registry")
            if std.startswith("lookup:"):
                if col.get("from_column") not in names:
                    err("V05", f"{s['table']}.{col['name']}: from_column {col.get('from_column')} không có trong columns")
                continue
            for c in feeding:
                src = col.get("source_by_system", {}).get(c["source_system"], col.get("source", col["name"]))
                if src is None:
                    continue
                if src not in {p["name"] for p in c["columns"]}:
                    err("V04", f"{s['table']}.{col['name']}: contract {c['id']} không có property '{src}'")
        for k in s["business_keys"] + s.get("event_keys", []):
            if k not in names:
                err("V05", f"{s['table']}: key {k} không có trong columns")
        for k in s["hash_exclude"]:
            if k not in names and k not in raws:
                err("V05", f"{s['table']}: hash_exclude {k} không có trong columns/keep_raw")
        if s["kind"] == "event" and not s.get("event_keys"):
            err("V05", f"{s['table']}: kind event cần event_keys")

    # V06
    for g in GOLD:
        for i in g["inputs"]:
            if i["table"] not in tables:
                err("V06", f"{g['table']}: input {i['table']} không có trong SILVER/GOLD")
            if i["read"] not in {"full", "cdf", "dep"}:
                err("V06", f"{g['table']}: input {i['table']} read '{i['read']}' không hợp lệ")
    graph = {g["table"]: [i["table"] for i in g["inputs"]] for g in GOLD}
    state = {}

    def visit(t, path):
        if state.get(t) == 2:
            return
        if state.get(t) == 1:
            err("V06", "chu trình: " + " -> ".join(path + [t]))
            return
        state[t] = 1
        for d in graph.get(t, []):
            visit(d, path + [t])
        state[t] = 2

    for t in graph:
        visit(t, [])

    # V07
    for o in objects:
        if not _is_pos_int(o["logic_version"]):
            err("V07", f"{o['table']}: logic_version phải là số nguyên >= 1")

    # V08
    seen = set()
    for c in CHECKS:
        cid = c.get("check_id")
        if cid in seen:
            err("V08", f"check_id {cid} trùng")
        seen.add(cid)
        if c.get("object") not in tables and c.get("object") not in META_TABLES:
            err("V08", f"{cid}: object {c.get('object')} không tồn tại")
        if c.get("type") not in CHECK_TYPES:
            err("V08", f"{cid}: type {c.get('type')} không hợp lệ")
        if c.get("severity") not in {"critical", "warning"}:
            err("V08", f"{cid}: severity {c.get('severity')} không hợp lệ")
        if c.get("scope") not in {"touched", "full", "run"}:
            err("V08", f"{cid}: scope {c.get('scope')} không hợp lệ")

    # V09
    for raw, std in ORDER_STATUS.items():
        if std not in STATUS_RANK:
            err("V09", f"ORDER_STATUS[{raw}] = {std} không có trong STATUS_RANK")

    # V10
    for k in sorted(SETTINGS_KEYS - set(SETTINGS)):
        err("V10", f"SETTINGS thiếu khoá {k}")

    if errors:
        raise ValueError("nb_config không hợp lệ:\n" + "\n".join(errors))
    return {"contracts": len(CONTRACTS), "silver": len(SILVER), "gold": len(GOLD), "checks": len(CHECKS)}


_summary = validate_config()
print("Validated {contracts} contracts, {silver} silver, {gold} gold, {checks} checks".format(**_summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

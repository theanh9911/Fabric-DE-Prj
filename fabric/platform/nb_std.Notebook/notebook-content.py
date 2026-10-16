# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_std
# Sinh biểu thức Spark SQL (chuỗi) chuẩn hoá cột và câu `SELECT` của bước conform. Python chỉ ghép chuỗi;
# biến đổi chạy bằng `spark.sql`. Không đọc/ghi bảng, không đọc `nb_config` (cấu hình truyền qua tham số).
# Spec: docs/05-thu-vien.md §3. Rule: R-STD-01…08, R-ORD-01, R-ORD-05.

# CELL ********************

import re

NULL_TOKENS = ("", "NULL", "null", "N/A", "n/a")

# Cột kỹ thuật `read_landing` thêm vào; conform giữ nguyên.
TECH_COLUMNS = ["source_system", "_batch_id", "_delivery_id", "_delivered_at_utc", "_snapshot_date",
                "_source_file", "_row_no"]

# Mã REJECT mặc định theo `std` khi cột `required` mà NULL (docs/05 §3.1).
DEFAULT_REJECT = {"code": "missing_business_key", "text": "missing_required_value",
                  "title": "missing_required_value", "ts": "invalid_timestamp", "date": "invalid_date",
                  "decimal": "invalid_number", "tax_rate": "invalid_number", "dict": "unknown_value"}


def _q(s):
    """Literal chuỗi Spark SQL: escape `\\` và `'` (không dùng `''` vì Spark nối hai literal liền nhau)."""
    return "'" + str(s).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _ident(name):
    return "`" + name.replace("`", "``") + "`"


def null_if_blank(c):
    toks = ", ".join(_q(t) for t in NULL_TOKENS)
    return f"CASE WHEN trim(CAST({c} AS STRING)) IN ({toks}) THEN NULL ELSE trim(CAST({c} AS STRING)) END"


def std_code(c):
    return f"upper(regexp_replace({null_if_blank(c)}, '\\\\s+', ''))"


def std_text(c):
    return f"regexp_replace({null_if_blank(c)}, '\\\\s+', ' ')"


def std_title(c):
    return f"initcap(lower({std_text(c)}))"


def parse_ts(c, formats, tz):
    x = null_if_blank(c)
    parts = ", ".join(f"try_to_timestamp({x}, {_q(f)})" for f in formats)
    return f"to_utc_timestamp(coalesce({parts}), {_q(tz)})"


def parse_date(c, formats):
    x = null_if_blank(c)
    parts = ", ".join(f"try_to_timestamp({x}, {_q(f)})" for f in formats)
    return f"to_date(coalesce({parts}))"


def is_ambiguous_dm(c):
    x = null_if_blank(c)
    pat = _q("^(\\\\d{1,2})/(\\\\d{1,2})/(\\\\d{4})")
    a = f"CAST(regexp_extract({x}, {pat}, 1) AS INT)"
    b = f"CAST(regexp_extract({x}, {pat}, 2) AS INT)"
    return f"coalesce(({x} RLIKE {pat} AND {a} <= 12 AND {b} <= 12 AND {a} <> {b}), false)"


def parse_decimal(c):
    x = null_if_blank(c)
    thousands = _q("^-?\\\\d{1,3}(,\\\\d{3})+(\\\\.\\\\d+)?$")
    return (f"try_cast(CASE WHEN {x} RLIKE {thousands} THEN regexp_replace({x}, ',', '') ELSE {x} END "
            f"AS DECIMAL(18,2))")


def dict_key(c):
    return f"upper(regexp_replace(CAST({c} AS STRING), '[^A-Za-z]', ''))"


def std_dict(c, mapping):
    whens = " ".join(f"WHEN {_q(k)} THEN {_q(v)}" for k, v in mapping.items())
    return f"CASE {dict_key(null_if_blank(c))} {whens} END"


def lookup_expr(c, mapping, cast_type):
    """Tra bảng theo giá trị canonical (khoá khớp đúng chữ, ví dụ STATUS_RANK['Pending'])."""
    whens = " ".join(f"WHEN {_q(k)} THEN {_q(v)}" for k, v in mapping.items())
    return f"CAST(CASE {c} {whens} END AS {cast_type})"


def parse_tax_rate(c, words):
    s = null_if_blank(c)
    num = f"try_cast(trim(regexp_replace({s}, '%', '')) AS DECIMAL(18,6))"
    word = f"try_cast({std_dict(c, words)} AS DECIMAL(18,6))"
    raw = f"coalesce({num}, {word})"
    val = f"CASE WHEN {raw} > 1 THEN {raw} / 100 ELSE {raw} END"
    return f"CASE WHEN ({val}) BETWEEN 0 AND 1 THEN CAST(({val}) AS DECIMAL(9,6)) END"


def record_hash(cols):
    parts = ", ".join(f"coalesce(CAST({_ident(c)} AS STRING), '<NULL>')" for c in sorted(cols))
    return f"sha2(concat_ws(char(31), {parts}), 256)"


def sk(*cols):
    return "xxhash64(" + ", ".join(f"CAST({c} AS STRING)" for c in cols) + ")"


def business_date(ts_utc, tz):
    return f"to_date(from_utc_timestamp({ts_utc}, {_q(tz)}))"


def date_key(d):
    return f"coalesce(CAST(date_format({d}, 'yyyyMMdd') AS INT), -1)"


def schema_drift(delivery_columns, contract):
    """Trả (added, missing, missing_required); tên so không phân biệt hoa thường, đã trim."""
    got = {c.strip().lower() for c in delivery_columns}
    want = {c["name"].strip().lower(): c for c in contract["columns"]}
    added = sorted(got - set(want))
    missing = sorted(set(want) - got)
    missing_required = sorted(n for n in missing if want[n]["required"])
    return added, missing, missing_required


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _source_name(col, source_system):
    by_system = col.get("source_by_system", {})
    if source_system in by_system:
        return by_system[source_system]
    return col.get("source", col["name"])


def _std_expr(col, src, contract, dictionaries):
    """Biểu thức canonical (chưa cast) cho một cột; `src` là tên cột nguồn đã chuẩn hoá hoặc None."""
    std = col["std"]
    if src is None:
        return "CAST(NULL AS STRING)"
    ref = _ident(src)
    if std == "code":
        return std_code(ref)
    if std == "text":
        return std_text(ref)
    if std == "title":
        return std_title(ref)
    if std == "ts":
        return parse_ts(ref, contract["timestamp_formats"], contract["source_timezone"])
    if std == "date":
        return parse_date(ref, contract["date_formats"])
    if std == "decimal":
        return parse_decimal(ref)
    if std == "tax_rate":
        return parse_tax_rate(ref, dictionaries["TAX_WORDS"])
    if std.startswith("dict:"):
        mapped = std_dict(ref, dictionaries[std.split(":", 1)[1]])
        # R-STD-08: không bắt buộc ⇒ không có trong từ điển thì giữ std_title (kèm FLAG)
        return mapped if col.get("required") else f"coalesce({mapped}, {std_title(ref)})"
    raise ValueError(f"std không hỗ trợ: {std}")


def conform_sql(contract, silver_spec, dictionaries, future_order_days, source_view, available_columns=None):
    """Một câu SELECT conform trên temp view `source_view` của MỘT source (docs/05 §3.3). Không lọc dòng.

    available_columns: tập tên cột (đã trim, chữ thường) có trong view; None ⇒ coi mọi property của contract
    đều có. Cột nguồn thiếu ⇒ NULL.
    """
    ss = contract["source_system"]
    have = set(available_columns) if available_columns is not None else {c["name"].lower() for c in contract["columns"]}
    cols = silver_spec["columns"]
    names = [c["name"] for c in cols]

    # cột nguồn dùng tới (trừ lookup)
    src_of = {}
    for c in cols:
        if c["std"].startswith("lookup:"):
            continue
        s = _source_name(c, ss)
        src_of[c["name"]] = s.strip().lower() if s is not None and s.strip().lower() in have else None
    used = sorted({s for s in src_of.values() if s})

    s_cols = ", ".join(f"{_ident(u)}" for u in used) if used else "1 AS _no_source_columns"
    tech = ", ".join(_ident(t) for t in TECH_COLUMNS)

    # c: cột canonical không phải lookup, và keep_raw
    c_items = []
    for c in cols:
        if c["std"].startswith("lookup:"):
            continue
        src = src_of[c["name"]]
        c_items.append(f"CAST({_std_expr(c, src, contract, dictionaries)} AS {c['type']}) AS {_ident(c['name'])}")
        if c.get("keep_raw"):
            raw = std_text(_ident(src)) if src else "CAST(NULL AS STRING)"
            c_items.append(f"{raw} AS {_ident(c['keep_raw'])}")
    # l: cột lookup
    l_items = []
    for c in cols:
        if c["std"].startswith("lookup:"):
            mapping = dictionaries[c["std"].split(":", 1)[1]]
            l_items.append(f"{lookup_expr(_ident(c['from_column']), mapping, c['type'])} AS {_ident(c['name'])}")

    # lý do REJECT, theo thứ tự cột
    rejects = []
    for c in cols:
        if not c.get("required"):
            continue
        base = c["std"].split(":", 1)[0]
        reason = c.get("reject_reason") or DEFAULT_REJECT.get(base, "missing_required_value")
        rejects.append(f"CASE WHEN {_ident(c['name'])} IS NULL THEN {_q(reason)} END")
    if "order_date" in names:
        rejects.append(f"CASE WHEN {_ident('order_date')} > date_add(to_date(_batch_id, 'yyyyMMdd'), "
                       f"{int(future_order_days)}) THEN 'future_order_date' END")
    reject_expr = "coalesce(" + ", ".join(rejects) + ")" if rejects else "CAST(NULL AS STRING)"

    # cờ FLAG: cờ dựa trên giá trị nguồn tính ở CTE c (nơi còn cột nguồn), cờ số âm tính ở r (trên cột canonical)
    src_flags, neg_flags = [], []
    for c in cols:
        if c["std"].startswith("lookup:"):
            continue
        src = src_of[c["name"]]
        n = _ident(c["name"])
        if src:
            present = f"{null_if_blank(_ident(src))} IS NOT NULL"
            if c.get("flag_reason"):
                cval = f"CAST({_std_expr(c, src, contract, dictionaries)} AS {c['type']})"
                src_flags.append(f"CASE WHEN {cval} IS NULL AND {present} THEN {_q(c['flag_reason'])} END")
            if c["std"].startswith("dict:") and not c.get("required"):
                mapped = std_dict(_ident(src), dictionaries[c["std"].split(":", 1)[1]])
                src_flags.append(f"CASE WHEN {mapped} IS NULL AND {present} THEN 'unmapped_value' END")
            if c["std"] in ("date", "ts"):
                src_flags.append(f"CASE WHEN {is_ambiguous_dm(_ident(src))} THEN 'ambiguous_date' END")
        if c.get("flag_negative"):
            neg_flags.append(f"CASE WHEN {n} < 0 THEN {_q(c['flag_negative'])} END")
    fl_names = [f"_fl{i}" for i in range(len(src_flags))]
    c_items += [f"{e} AS {n}" for e, n in zip(src_flags, fl_names)]
    flags_expr = "array_compact(array(" + ", ".join(["CAST(NULL AS STRING)"] + fl_names + neg_flags) + "))"

    # khoá và hash
    bk = "concat_ws('|', source_system, " + ", ".join(f"CAST({_ident(k)} AS STRING)" for k in silver_spec["business_keys"]) + ")"
    raw_cols = {c["keep_raw"] for c in cols if c.get("keep_raw")}
    hash_cols = [n for n in names if n not in set(silver_spec["hash_exclude"]) and n not in raw_cols]
    r_items = [f"{reject_expr} AS _reject_reason", f"{flags_expr} AS _dq_flags", f"{bk} AS _bk",
               f"{record_hash(hash_cols)} AS _record_hash"]
    out_cols = names + [c["keep_raw"] for c in cols if c.get("keep_raw")] + TECH_COLUMNS + [
        "_reject_reason", "_dq_flags", "_bk", "_record_hash"]
    if silver_spec["kind"] == "event":
        ek = "concat_ws('|', source_system, " + ", ".join(f"CAST({_ident(k)} AS STRING)" for k in silver_spec["event_keys"]) + ")"
        r_items.append(f"sha2({ek}, 256) AS _event_key")
        out_cols.append("_event_key")

    return (f"WITH s AS (SELECT {s_cols}, {tech} FROM {source_view}),\n"
            f"c AS (SELECT {', '.join(c_items)}, {tech} FROM s),\n"
            f"l AS (SELECT c.*{(', ' + ', '.join(l_items)) if l_items else ''} FROM c),\n"
            f"r AS (SELECT l.*, {', '.join(r_items)} FROM l)\n"
            f"SELECT {', '.join(_ident(c) for c in out_cols)} FROM r")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

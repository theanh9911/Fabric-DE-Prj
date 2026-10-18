# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_common
# Hạ tầng dùng chung: runtime, truy cập cấu hình, path, catalog, đọc Landing, ghi Delta, CDF, quarantine,
# `snapshot_changes`, trạng thái và quyết định chạy, kế hoạch ingest, kết quả worker, check, trạng thái hiện hành.
# Chỉ định nghĩa hàm; đặt cấu hình phiên Spark (duy nhất ở đây). Spec: docs/05-thu-vien.md §4. Đứng sau `nb_contracts`,
# `nb_config`, `nb_std`.

# CELL ********************

import fnmatch
import json
import re
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")


class CdfUnavailable(Exception):
    """Lịch sử CDF không còn (đã VACUUM) hoặc bảng chưa bật CDF ở version đó."""


# ---- 4.1 Runtime -------------------------------------------------------------------------------------------

def utc_now():
    return datetime.now(timezone.utc)


def new_id():
    return str(uuid.uuid4())


def default_load_date():
    return (datetime.now(ZoneInfo(SETTINGS["business_timezone"])).date() - timedelta(days=1)).isoformat()


def to_batch_id(load_date):
    try:
        return datetime.strptime(str(load_date), "%Y-%m-%d").strftime("%Y%m%d")
    except ValueError as e:
        raise ValueError(f"load_date không hợp lệ: {load_date!r} (cần yyyy-MM-dd)") from e


def parse_bool(x):
    if isinstance(x, bool):
        return x
    s = str(x).strip().lower()
    if s in ("true", "1"):
        return True
    if s in ("false", "0", ""):
        return False
    raise ValueError(f"không phải bool: {x!r}")


def json_param(s, default, name="param"):
    if s is None or (isinstance(s, str) and not s.strip()):
        return default
    if not isinstance(s, str):
        return s
    try:
        return json.loads(s)
    except ValueError as e:
        raise ValueError(f"{name}: JSON không hợp lệ: {e}") from e


def iso_utc(ms_or_dt):
    """epoch mili-giây hoặc datetime ⇒ 'yyyy-MM-ddTHH:mm:ssZ' (cắt phần giây lẻ)."""
    if isinstance(ms_or_dt, datetime):
        dt = ms_or_dt if ms_or_dt.tzinfo else ms_or_dt.replace(tzinfo=timezone.utc)
    else:
        dt = datetime.fromtimestamp(int(ms_or_dt) / 1000, tz=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---- 4.2 Truy cập cấu hình ---------------------------------------------------------------------------------

def get_contract(source_id):
    return CONTRACTS[source_id]


def active_contracts():
    return [c for c in CONTRACTS.values() if c["status"] != "retired"]


def contracts_for_silver(table):
    return [c for c in active_contracts() if c["silver_table"] == table]


def get_silver(table):
    for s in SILVER:
        if s["table"] == table:
            return s
    raise KeyError(table)


def get_gold(table):
    for g in GOLD:
        if g["table"] == table:
            return g
    raise KeyError(table)


def master_entities():
    return [s["entity"] for s in SILVER
            if s["kind"] == "snapshot" and s["is_active"] and contracts_for_silver(s["table"])]


def hash_columns(silver_spec):
    raw = {c["keep_raw"] for c in silver_spec["columns"] if c.get("keep_raw")}
    excl = set(silver_spec["hash_exclude"]) | raw
    return [c["name"] for c in silver_spec["columns"] if c["name"] not in excl]


# ---- 4.3 Tên và path ---------------------------------------------------------------------------------------

_TABLE_RE = re.compile(r"^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$")


def safe_table_name(name):
    if not isinstance(name, str) or not _TABLE_RE.match(name):
        raise ValueError(f"tên bảng không hợp lệ: {name!r}")
    return name


def staging_dir(delivery_id):
    return f"Files/_staging/{delivery_id}"


def landing_dir(batch_id, source_system, entity, delivery_id):
    return f"Files/landing/{batch_id}/{source_system}/{entity}/run={delivery_id}"


def temp_view(name):
    nb = globals().get("NB_NAME")
    if not nb:
        raise RuntimeError("NB_NAME chưa đặt")
    return f"_{nb}__{name}"


# ---- 4.4 Catalog và version --------------------------------------------------------------------------------

def table_exists(name):
    return spark.catalog.tableExists(safe_table_name(name))


def require_table(name):
    if not table_exists(name):
        raise RuntimeError(f"{name} missing — run nb_setup_ddl")


def _history(name, limit=None):
    q = f"DESCRIBE HISTORY {safe_table_name(name)}"
    if limit:
        q += f" LIMIT {int(limit)}"
    return spark.sql(q)


def table_version(name):
    return int(_history(name, 1).collect()[0]["version"])


_DATA_OPS = {"WRITE", "MERGE", "DELETE", "UPDATE", "CREATE OR REPLACE TABLE AS SELECT",
             "REPLACE TABLE AS SELECT", "CREATE TABLE AS SELECT", "TRUNCATE"}
_CHANGE_METRICS = ("numOutputRows", "numTargetRowsInserted", "numTargetRowsUpdated", "numTargetRowsDeleted",
                   "numDeletedRows", "numUpdatedRows")
_REWRITE_OPS = {"WRITE", "CREATE OR REPLACE TABLE AS SELECT", "REPLACE TABLE AS SELECT", "CREATE TABLE AS SELECT"}


def data_changed_since(name, version):
    """Có commit sau `version` làm đổi dữ liệu (OPTIMIZE, VACUUM, SET TBLPROPERTIES không tính)."""
    rows = (_history(name).where(F.col("version") > int(version))
            .select("operation", "operationMetrics").collect())
    for r in rows:
        if r["operation"] not in _DATA_OPS:
            continue
        m = r["operationMetrics"] or {}
        if sum(int(m.get(k) or 0) for k in _CHANGE_METRICS) > 0:
            return True
        if r["operation"] in _REWRITE_OPS and int(m.get("numRemovedFiles") or 0) > 0:
            return True
    return False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- 4.5 Landing -------------------------------------------------------------------------------------------

def _as_dict(d):
    return d.asDict() if hasattr(d, "asDict") else dict(d)


def _norm_columns(df):
    return df.toDF(*[c.strip().lower() for c in df.columns])


def read_csv_strings(paths, base_path=None):
    """Đọc CSV, mọi cột là chuỗi (04 §3.6); thêm `_source_file` (tên file) và `_row_no`."""
    r = (spark.read.option("header", True).option("multiLine", True).option("quote", '"')
         .option("escape", '"').option("mode", "PERMISSIVE").option("inferSchema", False)
         .option("encoding", "UTF-8"))
    if base_path:
        r = r.option("basePath", base_path)
    df = r.csv(paths)
    df = df.select("*", F.col("_metadata.file_name").alias("_source_file"))
    return _norm_columns(df).withColumn("_row_no", F.monotonically_increasing_id())


def _ts_literal(x):
    if isinstance(x, datetime):
        x = x if x.tzinfo else x.replace(tzinfo=timezone.utc)
        return x.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
    return str(x).replace("T", " ").replace("Z", "")


def read_landing(delivery):
    """Đọc một delivery từ Landing, mọi cột là chuỗi, thêm cột kỹ thuật của 05 §4.5."""
    d = _as_dict(delivery)
    path = d["landing_path"]
    if str(d["load_strategy"]).startswith("db_"):
        df = spark.read.option("basePath", path).parquet(path)
        df = df.select("*", F.col("_metadata.file_name").alias("_source_file"))
        df = df.select([F.col(c).cast("string").alias(c) for c in df.columns])
        df = _norm_columns(df).withColumn("_row_no", F.monotonically_increasing_id())
    else:
        df = read_csv_strings([path + "/*.csv"], base_path=path)
    delivered = d.get("delivered_at_str") or _ts_literal(d["delivered_at_utc"])
    snap = d.get("snapshot_date_str") or (str(d["snapshot_date"]) if d.get("snapshot_date") else None)
    return (df.withColumn("source_system", F.lit(d["source_system"]))
              .withColumn("_batch_id", F.lit(d["batch_id"]))
              .withColumn("_delivery_id", F.lit(d["delivery_id"]))
              .withColumn("_delivered_at_utc", F.lit(delivered).cast("timestamp"))
              .withColumn("_snapshot_date", F.lit(snap).cast("date")))


def list_files(path):
    """Mọi file (đệ quy) dưới `path`; thư mục không tồn tại ⇒ []."""
    if not notebookutils.fs.exists(path):
        return []
    out = []
    for f in notebookutils.fs.ls(path):
        if f.isDir:
            out.extend(list_files(f.path))
        else:
            out.append(f)
    return out


def promote_staging(delivery_id, landing_path, files=None):
    """Chuyển staging sang Landing. `files` (đường dẫn file) ⇒ chuyển từng file rồi xoá phần còn lại."""
    src = staging_dir(delivery_id)
    if files is None:
        return notebookutils.fs.mv(src, landing_path, True)
    for p in files:
        notebookutils.fs.mv(p, f"{landing_path}/{p.rsplit('/', 1)[-1]}", True)
    if notebookutils.fs.exists(src):
        notebookutils.fs.rm(src, True)
    return True


_MANIFEST_REQUIRED = ["manifest_version", "delivery_id", "run_id", "batch_id", "source_id", "source_system",
                      "entity", "contract_version", "load_strategy", "kind", "format", "window_start",
                      "window_end", "snapshot_date", "source_files", "rows_landed", "source_count",
                      "source_checksum", "columns", "delivered_at_utc"]


def write_manifest(landing_path, manifest):
    missing = [k for k in _MANIFEST_REQUIRED if k not in manifest]
    if missing:
        raise ValueError(f"manifest thiếu trường: {missing}")
    target = f"{landing_path}/_manifest.json"
    if notebookutils.fs.exists(target):
        raise RuntimeError(f"manifest đã tồn tại: {target}")
    notebookutils.fs.put(target, json.dumps(manifest, ensure_ascii=False, indent=2, default=str), False)


def read_manifest(landing_path):
    return json.loads(notebookutils.fs.head(f"{landing_path}/_manifest.json", 10_000_000))


# ---- 4.6 Ghi Delta -----------------------------------------------------------------------------------------

def align_to_target(target, df):
    fields = spark.table(safe_table_name(target)).schema.fields
    have = {c.lower(): c for c in df.columns}
    want = {f.name.lower() for f in fields}
    extra = [c for c in df.columns if c.lower() not in want]
    if extra:
        raise ValueError(f"{target}: cột thừa so với bảng: {extra}")
    cols = []
    for f in fields:
        if f.name.lower() in have:
            cols.append(F.col(f"`{have[f.name.lower()]}`").cast(f.dataType).alias(f.name))
        elif f.nullable:
            cols.append(F.lit(None).cast(f.dataType).alias(f.name))
        else:
            raise ValueError(f"{target}: thiếu cột NOT NULL {f.name}")
    return df.select(*cols)


def _view(df, name):
    v = temp_view(name)
    df.createOrReplaceTempView(v)
    return v


def _assert_unique(df, keys, target):
    dup = df.groupBy(*keys).count().where("count > 1").limit(5).collect()
    if dup:
        sample = [tuple(r[k] for k in keys) for r in dup]
        raise ValueError(f"{target}: input trùng khoá {keys}, mẫu: {sample}")


def overwrite_table(target, df):
    require_table(target)
    a = align_to_target(target, df)
    a.write.mode("overwrite").option("overwriteSchema", "false").saveAsTable(target)
    return last_commit_metrics(target)


def _merge(target, df, keys, update_cond, delete_missing, null_safe=False):
    require_table(target)
    a = align_to_target(target, df)
    _assert_unique(a, keys, target)
    v = _view(a, "mrg")
    cols = a.columns
    eq = "<=>" if null_safe else "="
    on = " AND ".join(f"t.`{k}` {eq} s.`{k}`" for k in keys)
    sql = f"MERGE INTO {target} t USING {v} s ON {on} "
    if update_cond:
        sql += (f"WHEN MATCHED AND {update_cond} THEN UPDATE SET "
                + ", ".join(f"t.`{c}` = s.`{c}`" for c in cols) + " ")
    sql += ("WHEN NOT MATCHED THEN INSERT (" + ", ".join(f"`{c}`" for c in cols) + ") VALUES ("
            + ", ".join(f"s.`{c}`" for c in cols) + ")")
    if delete_missing:
        sql += " WHEN NOT MATCHED BY SOURCE THEN DELETE"
    before = table_version(target)
    spark.sql(sql)
    m = last_commit_metrics(target)
    if m["version"] == before:  # MERGE không đổi gì có thể không tạo commit
        m = dict(m, operation="NOOP", rows_inserted=0, rows_updated=0, rows_deleted=0)
    return m


def merge_insert_only(target, df, keys):
    return _merge(target, df, keys, None, False, null_safe=True)


def merge_events(target, df):
    cond = "s._delivered_at_utc > t._delivered_at_utc AND s._record_hash <> t._record_hash"
    return _merge(target, df, ["_event_key"], cond, False)


def merge_upsert(target, df, keys, hash_col):
    return _merge(target, df, keys, f"t.`{hash_col}` <> s.`{hash_col}`", False)


def sync_table(target, df, keys, hash_col):
    return _merge(target, df, keys, f"t.`{hash_col}` <> s.`{hash_col}`", True)


def last_commit_metrics(target):
    r = _history(target, 1).collect()[0]
    m = dict(r["operationMetrics"] or {})
    op = r["operation"]

    def i(k):
        return int(m.get(k) or 0)

    out = {"version": int(r["version"]), "operation": op, "rows_inserted": 0, "rows_updated": 0, "rows_deleted": 0}
    if op == "MERGE":
        out.update(rows_inserted=i("numTargetRowsInserted"), rows_updated=i("numTargetRowsUpdated"),
                   rows_deleted=i("numTargetRowsDeleted"))
    elif op in _REWRITE_OPS:
        out["rows_inserted"] = i("numOutputRows")
    elif op == "DELETE":
        out["rows_deleted"] = i("numDeletedRows")
    elif op == "UPDATE":
        out["rows_updated"] = i("numUpdatedRows")
    return out


# ---- 4.7 CDF -----------------------------------------------------------------------------------------------

def read_changes(table, from_excl, to_incl):
    """Thay đổi trong (from_excl, to_incl]; dải rỗng ⇒ DataFrame rỗng đúng schema."""
    safe_table_name(table)
    if int(from_excl) >= int(to_incl):
        return (spark.table(table).limit(0)
                .withColumn("_change_type", F.lit(None).cast("string"))
                .withColumn("_commit_version", F.lit(None).cast("long"))
                .withColumn("_commit_timestamp", F.lit(None).cast("timestamp")))
    try:
        df = spark.sql(f"SELECT * FROM table_changes('{table}', {int(from_excl) + 1}, {int(to_incl)})")
        df.limit(1).collect()
        return df
    except Exception as e:  # noqa: BLE001 — mọi lỗi đọc CDF đều đưa về full
        raise CdfUnavailable(f"{table}: {str(e)[:300]}") from e


def changed_keys(table, rng, key_cols):
    return read_changes(table, rng[0], rng[1]).select(*key_cols).distinct()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- 4.8 Quarantine ----------------------------------------------------------------------------------------

_QUARANTINE_KEY = ["entity", "delivery_id", "business_key", "reason", "row_json"]
# Cột riêng của lượt chạy: nằm trong row_json thì xử lý lại (run_id mới) sẽ ghi trùng dòng quarantine.
_RUN_COLS = ("_run_id", "_logic_version", "_ingested_at_utc")


def quarantine_frame(df, entity, run_id):
    """DataFrame dòng quarantine (REJECT cho dòng bị loại, FLAG cho mỗi mã của dòng được nhận)."""
    payload = [c for c in df.columns if c not in ("_reject_reason", "_dq_flags", "_row_no") + _RUN_COLS]
    base = (df.withColumn("_row_json", F.to_json(F.struct(*[F.col(f"`{c}`") for c in payload])))
              .withColumn("_flags_arr", F.coalesce(F.col("_dq_flags"), F.array().cast("array<string>"))))

    def frame(d, reason_col, action):
        return d.select(F.lit(entity).alias("entity"), F.col("source_system"),
                        F.col("_batch_id").alias("batch_id"), F.col("_delivery_id").alias("delivery_id"),
                        F.col("_bk").alias("business_key"), reason_col.alias("reason"),
                        F.lit(action).alias("action"), F.col("_row_json").alias("row_json"),
                        F.lit(run_id).alias("run_id"), F.current_timestamp().alias("quarantined_at_utc"),
                        F.lit(None).cast("timestamp").alias("resolved_at_utc"),
                        F.lit(None).cast("string").alias("resolved_run_id"))

    rejects = frame(base.where(F.col("_reject_reason").isNotNull()), F.col("_reject_reason"), "REJECT")
    flags = frame(base.where(F.col("_reject_reason").isNull()).withColumn("_flag", F.explode("_flags_arr")),
                  F.col("_flag"), "FLAG")
    return rejects.unionByName(flags).dropDuplicates(_QUARANTINE_KEY)


def quarantine_rows(df, entity, run_id, table="slv.quarantine", replace=False):
    """`replace` (dựng lại hoàn toàn): quarantine là dữ liệu dẫn xuất từ Landing nên dòng cũ của các delivery trong
    `df` bị thay bằng dòng tính lại; nếu không, đổi logic rồi dựng lại sẽ để lại dòng không còn đúng."""
    q = quarantine_frame(df, entity, run_id).persist()
    n_reject = q.where("action = 'REJECT'").count()
    n_flag = q.where("action = 'FLAG'").count()
    if replace:
        ids = ", ".join(f"'{r[0]}'" for r in df.select("_delivery_id").distinct().collect())
        if ids:
            spark.sql(f"DELETE FROM {safe_table_name(table)} WHERE entity = '{entity}' AND delivery_id IN ({ids})")
    if n_reject + n_flag:
        merge_insert_only(table, q, _QUARANTINE_KEY)
    q.unpersist()
    return n_reject, n_flag


def resolve_quarantine(delivery_id, run_id, table="slv.quarantine"):
    """Đóng thủ công mọi dòng REJECT đang mở của một delivery (chỉ nb_ops_delivery gọi, L13). Không tự đóng theo
    khoá: dòng REJECT là một sự kiện chưa từng vào Silver, sự kiện hợp lệ khác của cùng khoá không sửa được nó."""
    spark.sql(f"UPDATE {safe_table_name(table)} SET resolved_at_utc = current_timestamp(), "
              f"resolved_run_id = '{run_id}' WHERE action = 'REJECT' AND resolved_at_utc IS NULL "
              f"AND delivery_id = '{delivery_id}'")


# ---- 4.9 snapshot_changes ----------------------------------------------------------------------------------

def snapshot_changes(snapshots, previous, key_cols):
    """Dòng thay đổi (INSERT/UPDATE/REINSERT/DELETE) của các snapshot mới so với `previous` (05 §4.9)."""
    cols = snapshots.columns
    if "is_deleted" not in cols or "_snapshot_date" not in cols or "_record_hash" not in cols:
        raise ValueError("snapshots cần is_deleted, _snapshot_date, _record_hash")
    sv = _view(snapshots, "snap")
    pv = _view(previous.select(*cols), "prev")
    keys = ", ".join(f"`{k}`" for k in key_cols)
    sel = ", ".join(f"`{c}`" for c in cols)
    del_sel = ", ".join("true AS `is_deleted`" if c == "is_deleted"
                        else ("_deleted_on AS `_snapshot_date`" if c == "_snapshot_date" else f"`{c}`")
                        for c in cols)
    sql = f"""
WITH s AS (SELECT {sel}, 0 AS _is_prev FROM {sv} UNION ALL SELECT {sel}, 1 AS _is_prev FROM {pv}),
dates AS (SELECT source_system, _snapshot_date,
                 dense_rank() OVER (PARTITION BY source_system ORDER BY _snapshot_date) AS _dseq
          FROM (SELECT DISTINCT source_system, _snapshot_date FROM {sv})),
maxseq AS (SELECT source_system, max(_dseq) AS _max_seq FROM dates GROUP BY source_system),
obs0 AS (SELECT s.*, CASE WHEN s._is_prev = 1 THEN 0 ELSE d._dseq END AS _seq
         FROM s LEFT JOIN dates d
           ON s.source_system = d.source_system AND s._snapshot_date = d._snapshot_date AND s._is_prev = 0),
obs AS (SELECT *, lag(_record_hash) OVER w AS _prev_hash, lag(is_deleted) OVER w AS _prev_deleted,
               lag(_seq) OVER w AS _prev_seq, lead(_seq) OVER w AS _next_seq
        FROM obs0 WINDOW w AS (PARTITION BY source_system, {keys} ORDER BY _seq)),
upserts AS (SELECT *, CASE WHEN _prev_hash IS NULL THEN 'INSERT'
                           WHEN _prev_deleted OR _seq > _prev_seq + 1 THEN 'REINSERT' ELSE 'UPDATE' END AS _change_kind
            FROM obs
            WHERE _is_prev = 0 AND (_prev_hash IS NULL OR _prev_deleted OR _seq > _prev_seq + 1
                                    OR _record_hash <> _prev_hash)),
deletes AS (SELECT o.*, d2._snapshot_date AS _deleted_on
            FROM obs o JOIN maxseq m ON o.source_system = m.source_system
            JOIN dates d2 ON d2.source_system = o.source_system AND d2._dseq = o._seq + 1
            WHERE NOT o.is_deleted AND o._seq < m._max_seq
              AND (o._next_seq IS NULL OR o._next_seq > o._seq + 1))
SELECT {sel}, _change_kind FROM upserts
UNION ALL
SELECT {del_sel}, 'DELETE' AS _change_kind FROM deletes
"""
    return spark.sql(sql)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- 4.10 Trạng thái và quyết định chạy -------------------------------------------------------------------

_RUN_LOG_DDL = ("run_id string, step string, layer string, event string, reason string, exec_mode string, "
                "run_mode string, batch_from string, batch_to string, logic_version int, "
                "input_versions map<string,bigint>, output_version bigint, rows_in bigint, rows_inserted bigint, "
                "rows_updated bigint, rows_deleted bigint, rows_rejected bigint, rows_flagged bigint, "
                "params string, event_at_utc timestamp, error_message string")
_RUN_LOG_COLS = [p.split()[0] for p in _RUN_LOG_DDL.split(", ")]


def _append_rows(table, rows, ddl):
    df = spark.createDataFrame(rows, ddl)
    align_to_target(table, df).write.mode("append").insertInto(table)


def log_run(run_id, step, layer, event, **cols):
    unknown = set(cols) - set(_RUN_LOG_COLS)
    if unknown:
        raise TypeError(f"log_run: cột không có trong meta.run_log: {sorted(unknown)}")
    row = {c: None for c in _RUN_LOG_COLS}
    row.update(cols)
    row.update(run_id=run_id, step=step, layer=layer, event=event, event_at_utc=utc_now())
    if isinstance(row["params"], (dict, list)):
        row["params"] = json.dumps(row["params"], ensure_ascii=False, default=str)
    if row["error_message"]:
        row["error_message"] = str(row["error_message"])[:4000]
    if row["input_versions"]:
        row["input_versions"] = {k: int(v["to"] if isinstance(v, dict) else v)
                                 for k, v in row["input_versions"].items()}
    _append_rows("meta.run_log", [tuple(row[c] for c in _RUN_LOG_COLS)], _RUN_LOG_DDL)


_CHECK_DDL = ("run_id string, batch_id string, check_id string, source string, check_type string, "
              "object_name string, scope_value string, expected decimal(38,4), actual decimal(38,4), "
              "threshold decimal(38,4), severity string, status string, message string, sample string, "
              "checked_at_utc timestamp")


def _dec(x):
    return None if x is None else Decimal(str(x)).quantize(Decimal("0.0001"))


def log_check(run_id, batch_id, check_id, source, check_type, object_name, scope_value, expected, actual,
              threshold, severity, message=None, sample=None, status=None):
    if status is None:
        e, a, t = _dec(expected) or Decimal(0), _dec(actual) or Decimal(0), _dec(threshold) or Decimal(0)
        status = "PASS" if abs(a - e) <= t else ("FAIL" if severity == "critical" else "WARN")
    row = (run_id, batch_id or None, check_id, source, check_type, object_name, scope_value, _dec(expected),
           _dec(actual), _dec(threshold), severity, status, message, None if sample is None else str(sample)[:2000],
           utc_now())
    _append_rows("meta.check_result", [row], _CHECK_DDL)
    return status


_DELIVERY_DDL = ("delivery_id string, status string, batch_id string, source_system string, entity string, "
                 "contract_version string, load_strategy string, kind string, landing_path string, "
                 "source_files array<string>, delivered_at_utc string, snapshot_date string, window_start string, "
                 "window_end string, rows_landed bigint, source_count bigint, source_checksum string, "
                 "schema_added array<string>, schema_missing array<string>, rows_read bigint, rows_accepted bigint, "
                 "rows_rejected bigint, rows_flagged bigint, logic_version int, run_id string, "
                 "event_at_utc timestamp, error_message string")
_DELIVERY_COLS = [p.split()[0] for p in _DELIVERY_DDL.split(", ")]
_SILVER_METRICS = {"schema_added", "schema_missing", "rows_read", "rows_accepted", "rows_rejected",
                   "rows_flagged", "logic_version", "error_message"}


def _file_names(files):
    return [f["name"] if isinstance(f, dict) else str(f) for f in (files or [])]


def record_delivery(delivery, status, run_id, **silver_metrics):
    if status not in ("LANDED", "LOADED", "FAILED", "PARKED"):
        raise ValueError(f"status không hợp lệ: {status}")
    unknown = set(silver_metrics) - _SILVER_METRICS
    if unknown:
        raise TypeError(f"record_delivery: tham số lạ {sorted(unknown)}")
    d = _as_dict(delivery)
    delivered = d.get("delivered_at_str") or d.get("delivered_at_utc")
    snap = d.get("snapshot_date_str") or d.get("snapshot_date")
    row = {c: None for c in _DELIVERY_COLS}
    row.update(delivery_id=d["delivery_id"], status=status, batch_id=d["batch_id"],
               source_system=d["source_system"], entity=d["entity"], contract_version=d.get("contract_version"),
               load_strategy=d.get("load_strategy"), kind=d.get("kind"), landing_path=d.get("landing_path"),
               source_files=_file_names(d.get("source_files")),
               delivered_at_utc=None if delivered is None else _ts_literal(delivered),
               snapshot_date=None if not snap else str(snap), window_start=d.get("window_start"),
               window_end=d.get("window_end"), rows_landed=d.get("rows_landed"), source_count=d.get("source_count"),
               source_checksum=d.get("source_checksum"), run_id=run_id, event_at_utc=utc_now())
    row.update(silver_metrics)
    _append_rows("meta.delivery_log", [tuple(row[c] for c in _DELIVERY_COLS)], _DELIVERY_DDL)


_LANDED_DDL = ("source_system string, entity string, file_name string, file_size bigint, modify_time_utc string, "
               "row_count bigint, file_checksum string, outcome string, delivery_id string, batch_id string, "
               "run_id string, landed_at_utc timestamp")


def record_landed_files(rows, run_id):
    out = []
    for r in rows:
        if r["outcome"] not in ("LANDED", "UNCHANGED", "SUPERSEDED"):
            raise ValueError(f"outcome không hợp lệ: {r['outcome']}")
        out.append((r["source_system"], r["entity"], r["file_name"], int(r["file_size"]),
                    _ts_literal(r["modify_time_utc"]), r.get("row_count"), r.get("file_checksum"), r["outcome"],
                    r.get("delivery_id"), r["batch_id"], run_id, utc_now()))
    if out:
        _append_rows("meta.landed_file", out, _LANDED_DDL)


_STATE_DDL = ("target_table string, logic_version int, input_versions map<string,bigint>, output_version bigint, "
              "last_run_id string, last_exec_mode string, last_success_at_utc timestamp")


def state_get(target):
    rows = (spark.table("meta.table_state").where(F.col("target_table") == target)
            .select("*", F.datediff(F.current_timestamp(), F.col("last_success_at_utc")).alias("age_days"))
            .limit(1).collect())
    if not rows:
        return None
    r = rows[0].asDict()
    r["input_versions"] = {k: int(v) for k, v in (r["input_versions"] or {}).items()}
    return r


def state_put(target, logic_version, input_versions, output_version, run_id, exec_mode):
    iv = {k: int(v["to"] if isinstance(v, dict) else v) for k, v in (input_versions or {}).items()}
    df = spark.createDataFrame([(target, int(logic_version), iv, int(output_version), run_id, exec_mode,
                                 utc_now())], _STATE_DDL)
    v = _view(df, "state")
    cols = ["target_table", "logic_version", "input_versions", "output_version", "last_run_id", "last_exec_mode",
            "last_success_at_utc"]
    spark.sql(f"MERGE INTO meta.table_state t USING {v} s ON t.target_table = s.target_table "
              f"WHEN MATCHED THEN UPDATE SET " + ", ".join(f"t.{c} = s.{c}" for c in cols)
              + " WHEN NOT MATCHED THEN INSERT (" + ", ".join(cols) + ") VALUES ("
              + ", ".join(f"s.{c}" for c in cols) + ")")


def select_deliveries(table, mode="normal", load_from=None, load_to=None, full=False):
    """Delivery Silver cần xử lý (04 §10.4), sắp theo snapshot_date, delivered_at_utc."""
    pairs = [(c["source_system"], c["entity"]) for c in contracts_for_silver(table)]
    if not pairs:
        return []
    cond = " OR ".join(f"(source_system = '{s}' AND entity = '{e}')" for s, e in pairs)
    if full:
        base, extra = "meta.v_delivery_state", " AND status <> 'PARKED'"
    elif mode == "rerun":
        base = "meta.v_delivery_state"
        extra = (f" AND status <> 'PARKED' AND batch_id >= '{to_batch_id(load_from)}'"
                 f" AND batch_id <= '{to_batch_id(load_to or load_from)}'")
    else:
        base, extra = "meta.v_pending_delivery", ""
    rows = spark.sql(
        f"SELECT *, date_format(delivered_at_utc, 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS delivered_at_str, "
        f"date_format(snapshot_date, 'yyyy-MM-dd') AS snapshot_date_str FROM {base} "
        f"WHERE ({cond}){extra} ORDER BY snapshot_date, delivered_at_utc, delivery_id").collect()
    return [r.asDict() for r in rows]


def _scope_items(scope):
    return [s.strip() for s in str(scope or "ALL").split(",") if s.strip()] or ["ALL"]


def decide_run(target, full_reload=False, scope="ALL", mode="normal", load_from=None, load_to=None):
    """Quyết định chạy một node Silver/Gold (04 §10.2). Trả decision, exec_mode, reason, input_versions."""
    t = safe_table_name(target)
    is_silver = t.startswith("slv.")
    spec = get_silver(t) if is_silver else get_gold(t)
    inputs = [] if is_silver else [i["table"] for i in spec["inputs"] if i["read"] in ("full", "cdf")]
    st = None

    def res(decision, exec_mode, reason):
        iv = {}
        for tbl in inputs:
            frm = st["input_versions"].get(tbl, -1) if st else -1
            iv[tbl] = {"from": frm, "to": table_version(tbl)}
        return {"decision": decision, "exec_mode": exec_mode, "reason": reason, "input_versions": iv}

    if not spec["is_active"]:
        return res("SKIP", "-", "inactive")
    items = _scope_items(scope)
    if parse_bool(full_reload) and ("ALL" in items or t in items):
        return res("RUN", "full", "full_reload")
    st = state_get(t)
    if st is None:
        return res("RUN", "full", "first_run")
    if int(st["logic_version"]) != int(spec["logic_version"]):
        return res("RUN", "full", "logic_version")
    if not is_silver:
        has_cdf = any(i["read"] == "cdf" for i in spec["inputs"])
        if has_cdf and (st["age_days"] is None or int(st["age_days"]) > SETTINGS["cdf_max_age_days"]):
            return res("RUN", "full", "checkpoint_too_old")
        for tbl in inputs:
            if data_changed_since(tbl, st["input_versions"].get(tbl, -1)):
                return res("RUN", "incremental", "input_changed")
        return res("SKIP", "-", "up_to_date")
    if select_deliveries(t, mode=mode, load_from=load_from, load_to=load_to):
        return res("RUN", "incremental", "pending_deliveries")
    return res("SKIP", "-", "up_to_date")


def resolve_source_ids(workspace_name, erp_item, drop_lakehouse):
    import sempy.fabric as fabric
    try:
        ws = fabric.resolve_workspace_id(workspace_name)
        erp = fabric.resolve_item_id(erp_item, "SQLDatabase", ws)  # cùng tên còn có item SQLEndpoint; Copy cần SQLDatabase
        drop = fabric.resolve_item_id(drop_lakehouse, "Lakehouse", ws)
    except Exception as e:  # noqa: BLE001
        raise ValueError(f"không tìm thấy workspace/item nguồn ({workspace_name}, {erp_item}, "
                         f"{drop_lakehouse}): {e}") from e
    if not (ws and erp and drop):
        raise ValueError(f"không tìm thấy workspace/item nguồn ({workspace_name}, {erp_item}, {drop_lakehouse})")
    return {"source_workspace_id": ws, "erp_item_id": erp, "drop_lakehouse_id": drop}


def platform_ids():
    """ID workspace và lakehouse mặc định của notebook đang chạy (cho Copy đích)."""
    import sempy.fabric as fabric
    return {"platform_workspace_id": fabric.get_notebook_workspace_id(),
            "platform_lakehouse_id": fabric.get_lakehouse_id()}


def list_drop_zone(ids, contract):
    root = (f"abfss://{ids['source_workspace_id']}@onelake.dfs.fabric.microsoft.com/"
            f"{ids['drop_lakehouse_id']}/Files/{contract['physical_name']}")
    if not notebookutils.fs.exists(root):
        return []
    out = []
    for f in notebookutils.fs.ls(root):
        if f.isDir:
            continue
        if f.size is None or f.modifyTime is None:
            raise RuntimeError(f"{contract['id']}: file {f.name} thiếu size hoặc modifyTime")
        if fnmatch.fnmatch(f.name, contract["file_glob"] or "*"):
            out.append({"name": f.name, "size": int(f.size), "modify_ms": int(f.modifyTime)})
    return out

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- Kế hoạch ingest và preflight (04 §3.2–3.4, §9.1, §11.2) ----------------------------------------------

_DATE_IN_NAME = re.compile(r"(\d{8})")
_MIN_WM = "1900-01-01 00:00:00"


def _fmt_dt(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _parse_wm(s):
    return datetime.strptime(str(s)[:19], "%Y-%m-%d %H:%M:%S")


def _db_window(c, load_date, wm, run_start_utc):
    if c["source_clock"] == "virtual":
        end = datetime.strptime(load_date, "%Y-%m-%d") + timedelta(days=1)
    else:
        local = run_start_utc.astimezone(ZoneInfo(c["source_timezone"])).replace(tzinfo=None)
        end = local - timedelta(minutes=int(c["commit_lag_minutes"] or 0))
    start = _parse_wm(wm or _MIN_WM) - timedelta(minutes=int(c["lookback_minutes"] or 0))
    return start, end


def plan_ingest(contracts, load_date, watermarks, landed_files, listings, run_start_utc, batch_id,
                id_fn=new_id):
    """Hàm thuần: ⇒ {sources, no_data, errors, superseded}.

    watermarks: {(source_system, entity): 'yyyy-MM-dd HH:mm:ss'}
    landed_files: tập (source_system, entity, name, size, modify_epoch_seconds)
    listings: {source_id: [{name, size, modify_ms}]}
    """
    sources, no_data, errors, superseded = [], [], [], []
    for c in contracts:
        sid, ss, en = c["id"], c["source_system"], c["entity"]
        base = {"source_id": sid, "source_system": ss, "entity": en, "load_strategy": c["load_strategy"],
                "kind": c["kind"], "contract_version": c["version"], "batch_id": batch_id}
        if c["load_strategy"].startswith("db_"):
            delivery_id = id_fn()
            item = dict(base, delivery_id=delivery_id, staging_path=staging_dir(delivery_id),
                        staging_folder=f"_staging/{delivery_id}", window_start=None, window_end=None,
                        snapshot_date=load_date if c["kind"] == "snapshot" else None)
            if c["load_strategy"] == "db_incremental":
                start, end = _db_window(c, load_date, watermarks.get((ss, en)), run_start_utc)
                if start >= end:
                    no_data.append({"source_id": sid, "reason": "up_to_date"})
                    continue
                wm = c["watermark_column"]
                item.update(window_start=_fmt_dt(start), window_end=_fmt_dt(end),
                            query=(f"SELECT * FROM {c['physical_name']} WHERE [{wm}] >= '{_fmt_dt(start)}' "
                                   f"AND [{wm}] < '{_fmt_dt(end)}'"))
            else:
                item["query"] = f"SELECT * FROM {c['physical_name']}"
            sources.append(item)
            continue

        files = list(listings.get(sid, []))
        settle_ms = int(c["settle_minutes"] or 0) * 60_000
        cutoff_ms = int(run_start_utc.timestamp() * 1000) - settle_ms
        settled = [f for f in files if f["modify_ms"] <= cutoff_ms]
        pending_settle = len(files) - len(settled)
        fresh = [f for f in settled if (ss, en, f["name"], f["size"], f["modify_ms"] // 1000) not in landed_files]
        snapshot_date = None
        if c["kind"] == "snapshot" and fresh:
            bad = [f["name"] for f in fresh if not _DATE_IN_NAME.search(f["name"])]
            if bad:
                errors.append({"source_id": sid, "error": f"file snapshot không có ngày yyyyMMdd trong tên: {bad}"})
                continue
            newest = max(_DATE_IN_NAME.search(f["name"]).group(1) for f in fresh)
            for f in fresh:
                if _DATE_IN_NAME.search(f["name"]).group(1) != newest:
                    superseded.append({"source_system": ss, "entity": en, "file_name": f["name"],
                                       "file_size": f["size"], "modify_time_utc": iso_utc(f["modify_ms"]),
                                       "outcome": "SUPERSEDED", "batch_id": batch_id})
            fresh = [f for f in fresh if _DATE_IN_NAME.search(f["name"]).group(1) == newest]
            snapshot_date = f"{newest[:4]}-{newest[4:6]}-{newest[6:]}"
        if not fresh:
            reason = f"settle_pending:{pending_settle}" if pending_settle else "no_new_files"
            no_data.append({"source_id": sid, "reason": reason})
            continue
        delivery_id = id_fn()
        sources.append(dict(base, delivery_id=delivery_id, staging_path=staging_dir(delivery_id),
                            staging_folder=f"_staging/{delivery_id}", source_folder=c["physical_name"],
                            file_glob=c["file_glob"], window_start=None, window_end=None,
                            snapshot_date=snapshot_date,
                            modified_start=iso_utc(min(f["modify_ms"] for f in fresh)),
                            modified_end=iso_utc(max(f["modify_ms"] for f in fresh) + 1000),
                            expected_files=[{"name": f["name"], "size": f["size"],
                                             "modify_time_utc": iso_utc(f["modify_ms"])} for f in fresh]))
    return {"sources": sources, "no_data": no_data, "errors": errors, "superseded": superseded}


def preflight(p_mode, p_load_date, p_load_date_to, p_scope, p_full_reload):
    """PF01–PF08; trả tham số đã chuẩn hoá. Sai ⇒ ValueError nêu mã."""
    mode = str(p_mode or "normal").strip()
    load_date = str(p_load_date or "").strip()
    load_to = str(p_load_date_to or "").strip()
    full = parse_bool(p_full_reload)
    if mode not in ("normal", "rerun"):
        raise ValueError(f"PF01: p_mode phải là normal hoặc rerun, nhận {mode!r}")
    for name, val in (("p_load_date", load_date), ("p_load_date_to", load_to)):
        if val:
            try:
                datetime.strptime(val, "%Y-%m-%d")
            except ValueError as e:
                raise ValueError(f"PF02: {name} sai định dạng yyyy-MM-dd: {val!r}") from e
    if load_to and not load_date:
        raise ValueError("PF02: có p_load_date_to mà không có p_load_date")
    if load_to and load_to < load_date:
        raise ValueError("PF02: p_load_date_to < p_load_date")
    if mode == "rerun" and not full and not load_date:
        raise ValueError("PF03: rerun không full_reload cần p_load_date")
    if full and mode != "rerun":
        raise ValueError("PF04: p_full_reload chỉ dùng với p_mode = rerun")
    if full and load_date:
        raise ValueError("PF05: p_full_reload là toàn bộ lịch sử, không kèm p_load_date")
    if load_to and mode == "normal":
        raise ValueError("PF06: p_load_date_to chỉ dùng với p_mode = rerun")
    items = _scope_items(p_scope)
    valid = {"ALL"} | set(CONTRACTS) | {s["table"] for s in SILVER} | {g["table"] for g in GOLD}
    bad = [s for s in items if s not in valid]
    if bad:
        raise ValueError(f"PF07: p_scope có phần tử không hợp lệ: {bad}")
    try:
        validate_config()
    except Exception as e:  # noqa: BLE001
        raise ValueError(f"PF08: validate_config lỗi: {e}") from e
    if mode == "normal" and not load_date:
        load_date = default_load_date()
    if mode == "rerun" and load_date and not load_to:
        load_to = load_date
    check_scope = "daily"
    if full or (load_date and datetime.strptime(load_date, "%Y-%m-%d").weekday() == SETTINGS["full_checks_weekday"]):
        check_scope = "full"
    return {"mode": mode, "load_date": load_date or None, "load_date_to": load_to or None, "scope": items,
            "full_reload": full, "check_scope": check_scope,
            "batch_id": to_batch_id(load_date) if load_date else ""}


# ---- 4.11 Worker -------------------------------------------------------------------------------------------

def worker_result(target, exec_mode, status="SUCCEEDED", reason=None, **metrics):
    keys = ("rows_in", "rows_inserted", "rows_updated", "rows_deleted", "rows_rejected", "rows_flagged")
    r = {"contract": 1, "status": status, "target": target, "exec_mode": exec_mode, "reason": reason}
    for k in keys:
        r[k] = int(metrics.get(k) or 0)
    r["output_version"] = table_version(target) if table_exists(target) else None
    return r


def exit_worker(result):
    notebookutils.notebook.exit(json.dumps(result, default=str))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- 4.12 Check ------------------------------------------------------------------------------------------

def _run_list(run_ids):
    return ", ".join(f"'{r}'" for r in run_ids)


def unchecked_run_ids(run_id):
    """run_id của mọi lượt có sự kiện sau lần `dq` SUCCEEDED gần nhất, cộng lượt hiện tại. Lượt chạy lại sau lỗi có
    run_id mới, nên check chỉ theo run_id hiện tại sẽ bỏ sót phần của lượt lỗi (PASS rỗng)."""
    rows = spark.sql(
        "SELECT DISTINCT run_id FROM meta.run_log WHERE event_at_utc > coalesce("
        "(SELECT max(event_at_utc) FROM meta.run_log WHERE step = 'dq' AND event = 'SUCCEEDED' "
        f"AND run_id <> '{run_id}'), TIMESTAMP'1900-01-01 00:00:00')").collect()
    return sorted({r["run_id"] for r in rows} | {run_id})


def source_delivered(c, batch_id):
    """Nguồn đã giao cho batch: có delivery LANDED/LOADED, hoặc lần ingest báo UNCHANGED / zero_rows / up_to_date
    (nguồn giao đủ, chỉ là không có gì mới). File không tới (no_staging_files, plan_mismatch) thì chưa giao."""
    n = spark.sql(
        "SELECT count(*) AS n FROM meta.v_delivery_state WHERE status IN ('LANDED', 'LOADED') "
        f"AND source_system = '{c['source_system']}' AND entity = '{c['entity']}' "
        f"AND batch_id = '{batch_id}'").collect()[0]["n"]
    if n:
        return True
    return spark.sql(
        f"SELECT count(*) AS n FROM meta.run_log WHERE step = 'ingest.{c['id']}' AND batch_from = '{batch_id}' "
        "AND (event = 'UNCHANGED' OR (event = 'NO_DATA' AND reason IN ('zero_rows', 'up_to_date')))"
        ).collect()[0]["n"] > 0


def _where(spec, run_ids, *conds, alias=""):
    p = f"{alias}." if alias else ""
    parts = ([f"{p}_run_id IN ({_run_list(run_ids)})"] if spec["scope"] == "touched" else []) + list(conds)
    return ("WHERE " + " AND ".join(parts)) if parts else ""


def _eq(cols, a="t", b="r"):
    return " AND ".join(f"{a}.`{c}` <=> {b}.`{c}`" for c in cols)


def _single(sql):
    r = spark.sql(sql).collect()[0].asDict()
    return r["actual"], r.get("sample")


def _check_unique(spec, run_ids):
    cols = spec["params"]["cols"]
    keys = ", ".join(f"`{c}`" for c in cols)
    obj = spec["object"]
    if spec["scope"] == "touched":
        inner = (f"SELECT t.* FROM {obj} t LEFT SEMI JOIN (SELECT DISTINCT {keys} FROM {obj} "
                 f"WHERE _run_id IN ({_run_list(run_ids)})) r ON {_eq(cols)}")
    else:
        inner = f"SELECT * FROM {obj}"
    cast = ", ".join(f"CAST(`{c}` AS STRING)" for c in cols)
    return _single(f"SELECT count(*) AS actual, "
                   f"concat_ws('; ', slice(collect_list(concat_ws('|', {cast})), 1, 5)) AS sample "
                   f"FROM (SELECT {keys} FROM ({inner}) GROUP BY {keys} HAVING count(*) > 1)")


def _check_not_null(spec, run_ids):
    cond = "(" + " OR ".join(f"`{c}` IS NULL" for c in spec["params"]["cols"]) + ")"
    return _single(f"SELECT count(*) AS actual, CAST(NULL AS STRING) AS sample FROM {spec['object']} "
                   f"{_where(spec, run_ids, cond)}")


def _check_flag_count(spec, run_ids):
    flags = ", ".join(f"'{f}'" for f in spec["params"]["flags"])
    cond = f"size(array_intersect(_dq_flags, array({flags}))) > 0"
    return _single(f"SELECT count(*) AS actual, CAST(NULL AS STRING) AS sample FROM {spec['object']} "
                   f"{_where(spec, run_ids, cond)}")


def _check_scd2_one_current(spec, run_ids):
    return _single("SELECT count(*) AS actual, CAST(NULL AS STRING) AS sample FROM ("
                   f"SELECT source_system, salesman_code FROM {spec['object']} GROUP BY source_system, salesman_code "
                   "HAVING sum(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1)")


def _check_scd2_contiguous(spec, run_ids):
    return _single("SELECT count(*) AS actual, CAST(NULL AS STRING) AS sample FROM ("
                   "SELECT valid_to, lead(valid_from) OVER (PARTITION BY source_system, salesman_code "
                   f"ORDER BY valid_from) AS nxt FROM {spec['object']}) "
                   "WHERE nxt IS NOT NULL AND nxt <> date_add(valid_to, 1)")


def _check_unknown_member_rate(spec, run_ids):
    w = _where(spec, run_ids, alias="f")
    return _single(
        "SELECT coalesce(100.0 * sum(CASE WHEN f.customer_sk = -1 OR f.product_sk = -1 OR f.salesman_sk = -1 "
        "OR c.is_inferred OR p.is_inferred OR s.is_inferred THEN 1 ELSE 0 END) / nullif(count(*), 0), 0) AS actual, "
        f"CAST(NULL AS STRING) AS sample FROM {spec['object']} f "
        "LEFT JOIN gld.dim_customer c ON f.customer_sk = c.customer_sk "
        "LEFT JOIN gld.dim_product p ON f.product_sk = p.product_sk "
        f"LEFT JOIN gld.dim_salesman s ON f.salesman_sk = s.salesman_sk {w}")


def _check_sk_collision(spec, run_ids):
    sk = spec["params"]["sk"]
    nat = ", ".join(f"CAST(`{c}` AS STRING)" for c in spec["params"]["natural"])
    return _single(f"SELECT count(*) AS actual, CAST(NULL AS STRING) AS sample FROM ("
                   f"SELECT `{sk}` FROM {spec['object']} GROUP BY `{sk}` "
                   f"HAVING count(DISTINCT concat_ws('|', {nat})) > 1)")


def _check_duplicate_file(spec, run_ids):
    return _single(
        "SELECT count(*) AS actual, CAST(NULL AS STRING) AS sample FROM meta.landed_file a "
        f"WHERE a.run_id IN ({_run_list(run_ids)}) AND a.outcome = 'LANDED' AND a.file_checksum IS NOT NULL AND EXISTS ("
        "SELECT 1 FROM meta.landed_file b WHERE b.source_system = a.source_system AND b.entity = a.entity "
        "AND b.file_checksum = a.file_checksum AND b.file_name <> a.file_name)")


def _check_parked(spec, run_ids):
    return _single("SELECT count(*) AS actual, CAST(NULL AS STRING) AS sample FROM meta.v_delivery_state "
                   "WHERE status = 'PARKED'")


def _check_schema_drift(spec, run_ids):
    return _single("SELECT count(*) AS actual, CAST(NULL AS STRING) AS sample FROM meta.v_delivery_state "
                   f"WHERE run_id IN ({_run_list(run_ids)}) AND status IN ('LOADED', 'FAILED') "
                   "AND (size(schema_added) > 0 OR size(schema_missing) > 0)")


def _check_event_date_vs_file(spec, run_ids):
    tz = " ".join(f"WHEN '{c['source_system']}' THEN '{c['source_timezone']}'"
                  for c in active_contracts() if c["silver_table"] == spec["object"]
                  and c["load_strategy"].startswith("file_"))
    if not tz:
        return 0, None
    cond = "_source_file RLIKE '^orders_[0-9]{8}[.]csv$'"
    lag = (f"abs(datediff(to_date(from_utc_timestamp(updated_at_utc, CASE source_system {tz} END)), "
           "to_date(regexp_extract(_source_file, '([0-9]{8})', 1), 'yyyyMMdd'))) > 1")
    return _single(f"SELECT count(*) AS actual, CAST(NULL AS STRING) AS sample FROM {spec['object']} "
                   f"{_where(spec, run_ids, cond, lag)}")

def _quarantine_rate(spec, run_id, run_ids):
    rows = spark.sql(
        "SELECT delivery_id, batch_id, source_system, entity, rows_read, rows_rejected FROM meta.v_delivery_state "
        f"WHERE run_id IN ({_run_list(run_ids)}) AND status IN ('LOADED', 'FAILED') AND rows_read > 0").collect()
    worst = "PASS"
    for r in rows:
        c = get_contract(f"{r['source_system']}.{r['entity']}")
        pct = 100.0 * (r["rows_rejected"] or 0) / r["rows_read"]
        warn, fail = c["quarantine_warn_pct"], c["quarantine_fail_pct"]
        st = "PASS" if pct <= warn else ("WARN" if pct <= fail else "FAIL")
        log_check(run_id, r["batch_id"], spec["check_id"], "platform", spec["type"],
                  f"{r['source_system']}.{r['entity']}", r["delivery_id"], 0, pct, warn, spec["severity"],
                  message=f"rows_rejected/rows_read = {pct:.2f}% (warn {warn}%, fail {fail}%)", status=st)
        worst = "FAIL" if st == "FAIL" else ("WARN" if st == "WARN" and worst != "FAIL" else worst)
    return worst


def _freshness(spec, batch_id):
    late = [c["id"] for c in active_contracts()
            if c["delivery_cadence"] == "daily" and not source_delivered(c, batch_id)]
    return len(late), ", ".join(late)


_CHECK_FUNCS = {"unique": _check_unique, "not_null": _check_not_null, "flag_count": _check_flag_count,
                "scd2_one_current": _check_scd2_one_current, "scd2_contiguous": _check_scd2_contiguous,
                "unknown_member_rate": _check_unknown_member_rate, "sk_collision": _check_sk_collision,
                "duplicate_file": _check_duplicate_file, "parked_delivery": _check_parked,
                "schema_drift": _check_schema_drift, "event_date_vs_file": _check_event_date_vs_file}


def run_check(spec, run_id, batch_id, run_scope, run_ids=None):
    """Chạy một check của CHECKS, ghi `meta.check_result`, trả status (None nếu bỏ qua theo phạm vi lượt).
    `run_ids`: các lượt chưa được kiểm (`unchecked_run_ids`); mặc định chỉ `run_id`."""
    if spec["scope"] == "full" and run_scope != "full":
        return None
    run_ids = run_ids or [run_id]
    t = spec["type"]
    if t == "quarantine_rate":
        return _quarantine_rate(spec, run_id, run_ids)
    if t == "freshness":
        actual, sample = _freshness(spec, batch_id)
    else:
        actual, sample = _CHECK_FUNCS[t](spec, run_ids)
    thr = spec.get("threshold", 0)
    return log_check(run_id, batch_id, spec["check_id"], "platform", t, spec["object"], spec["scope"], 0, actual,
                     thr, spec["severity"], message=spec.get("description"), sample=sample)


def run_contract_checks(deliveries, run_id):
    """Contract quality (03 §3.5) trên từng delivery; trả [(check_id, delivery_id, status)]."""
    out = []
    for i, dv in enumerate(deliveries):
        d = _as_dict(dv)
        c = get_contract(f"{d['source_system']}.{d['entity']}")
        if not c["quality"]:
            continue
        view = temp_view(f"ct_{i}")
        read_landing(d).createOrReplaceTempView(view)
        for q in c["quality"]:
            actual = spark.sql(q["query"].replace("${object}", view)).collect()[0][0]
            sev = "critical" if q["severity"] == "error" else "warning"
            st = log_check(run_id, d["batch_id"], q["check_id"], "contract", "sql", c["id"], d["delivery_id"],
                           q["must_be"], actual, 0, sev, message=q["description"])
            out.append((q["check_id"], d["delivery_id"], st))
    return out


# ---- 4.13 Trạng thái hiện hành ----------------------------------------------------------------------------

def latest_event(events):
    w = Window.partitionBy("source_system", "order_no", "product_code").orderBy(
        F.col("updated_at_utc").desc(), F.col("status_rank").desc(), F.col("_delivered_at_utc").desc(),
        F.col("_row_no").desc())
    return events.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


# ---- Silver: conform một delivery ---------------------------------------------------------------------------

_CONFORM_SEQ = [0]


def conform_delivery(delivery, silver_spec):
    """Đọc Landing của delivery và conform bằng SQL sinh từ cấu hình (05 §3.3); không lọc dòng."""
    d = _as_dict(delivery)
    contract = get_contract(f"{d['source_system']}.{d['entity']}")
    raw = read_landing(d)
    _CONFORM_SEQ[0] += 1
    view = temp_view(f"raw{_CONFORM_SEQ[0]}")
    raw.createOrReplaceTempView(view)
    avail = {c for c in raw.columns if not c.startswith("_") and c != "source_system"}
    sql = conform_sql(contract, silver_spec, DICTIONARIES, SETTINGS["future_order_days"], view,
                      available_columns=avail)
    return spark.sql(sql)


def delivery_schema(delivery):
    """(added, missing, missing_required) của delivery so với contract, theo manifest.columns."""
    d = _as_dict(delivery)
    contract = get_contract(f"{d['source_system']}.{d['entity']}")
    cols = read_manifest(d["landing_path"])["columns"]
    return schema_drift(cols, contract)


def deliveries_of_run(run_id, statuses=("LANDED", "LOADED"), table="meta.v_delivery_state"):
    """Delivery có sự kiện trạng thái thuộc `run_id` (mặc định: trạng thái mới nhất của delivery)."""
    st = ", ".join(f"'{x}'" for x in statuses)
    rows = spark.sql(
        f"SELECT *, date_format(delivered_at_utc, 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS delivered_at_str, "
        f"date_format(snapshot_date, 'yyyy-MM-dd') AS snapshot_date_str FROM {safe_table_name(table)} "
        f"WHERE run_id = '{run_id}' AND status IN ({st}) ORDER BY source_system, entity, delivered_at_utc").collect()
    return [r.asDict() for r in rows]


def rebuilt_since_last_recon():
    """Silver/Gold được dựng lại hoàn toàn (exec_mode full) sau lần `recon` SUCCEEDED gần nhất: bảng đã đổi nên kết
    quả đối soát cũ của delivery không còn giá trị, phải đối soát lại."""
    return spark.sql(
        "SELECT count(*) AS n FROM meta.run_log WHERE exec_mode = 'full' AND event = 'SUCCEEDED' "
        "AND (step LIKE 'slv.%' OR step LIKE 'gld.%') AND event_at_utc > coalesce("
        "(SELECT max(event_at_utc) FROM meta.run_log WHERE step = 'recon' AND event = 'SUCCEEDED'), "
        "TIMESTAMP'1900-01-01 00:00:00')").collect()[0]["n"] > 0


def deliveries_to_reconcile(check_id, statuses, table="meta.v_delivery_state", latest_per_key=False, source="recon",
                            include_checked=False):
    """Delivery chưa có kết quả `source` (recon/contract) cho `check_id` (None: bất kỳ check nào của source) không
    FAIL, bất kể thuộc lượt nào: lượt chạy lại sau lỗi có `run_id` mới nên không thấy delivery của lượt lỗi.
    `latest_per_key`: chỉ delivery mới nhất của mỗi (source_system, entity), chọn trước khi loại các delivery
    đã kiểm. `include_checked`: bỏ điều kiện "chưa kiểm" (đối soát lại tất cả sau khi bảng được dựng lại)."""
    st = ", ".join(f"'{x}'" for x in statuses)
    only = f"AND c.check_id = '{check_id}' " if check_id else ""
    conds = (["_rk = 1"] if latest_per_key else []) + ([] if include_checked else [
        f"NOT EXISTS (SELECT 1 FROM meta.check_result c WHERE c.source = '{source}' {only}"
        f"AND c.scope_value = d.delivery_id AND c.status <> 'FAIL')"])
    where = ("WHERE " + " AND ".join(conds)) if conds else ""
    rows = spark.sql(
        f"WITH d AS (SELECT *, row_number() OVER (PARTITION BY source_system, entity "
        f"ORDER BY snapshot_date DESC NULLS LAST, delivered_at_utc DESC) AS _rk FROM {safe_table_name(table)} "
        f"WHERE status IN ({st})) "
        f"SELECT d.*, date_format(delivered_at_utc, 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS delivered_at_str, "
        f"date_format(snapshot_date, 'yyyy-MM-dd') AS snapshot_date_str FROM d {where} "
        f"ORDER BY source_system, entity, delivered_at_utc").collect()
    return [r.asDict() for r in rows]


def conform_master(delivery, silver_spec):
    """Conform một snapshot master: R-MST-01 (trùng khoá ⇒ giữ `_row_no` nhỏ nhất, dòng còn lại FLAG
    `duplicate_key_in_snapshot` và `_is_dup`), `self_manager` cho sales_hierarchy."""
    df = conform_delivery(delivery, silver_spec)
    flags = F.coalesce(F.col("_dq_flags"), F.array().cast("array<string>"))
    cols = {c["name"] for c in silver_spec["columns"]}
    if {"salesmanager_code", "salesman_code"} <= cols:
        selfm = F.col("salesmanager_code").isNotNull() & (F.col("salesmanager_code") == F.col("salesman_code"))
        df = df.withColumn("_dq_flags", F.when(selfm, F.array_union(flags, F.array(F.lit("self_manager"))))
                           .otherwise(flags))
        df = df.withColumn("salesmanager_code", F.when(selfm, F.lit(None).cast("string"))
                           .otherwise(F.col("salesmanager_code")))
        df = df.withColumn("_record_hash", F.expr(record_hash(hash_columns(silver_spec))))
        flags = F.col("_dq_flags")
    w = Window.partitionBy("_bk", F.col("_reject_reason").isNull()).orderBy(F.col("_row_no"))
    dup = F.col("_reject_reason").isNull() & (F.row_number().over(w) > 1)
    return (df.withColumn("_is_dup", dup)
              .withColumn("_dq_flags", F.when(dup, F.array_union(flags, F.array(F.lit("duplicate_key_in_snapshot"))))
                          .otherwise(flags)))


def run_node(target, build, run_id, full_reload=False, scope="ALL", mode="normal", load_from=None, load_to=None):
    """Khung chạy một node Silver/Gold: decide_run → build(decision) → state_put → log_run → worker_result.

    `build(decision)` ghi dữ liệu và trả dict số liệu (rows_in, rows_inserted, rows_updated, rows_deleted,
    rows_rejected, rows_flagged; tuỳ chọn exec_mode, reason, skip_state)."""
    layer = "slv" if target.startswith("slv.") else "gld"
    spec = get_silver(target) if layer == "slv" else get_gold(target)
    decision = decide_run(target, full_reload, scope, mode, load_from, load_to)
    if decision["decision"] == "SKIP":
        log_run(run_id, target, layer, "SKIPPED", reason=decision["reason"])
        return worker_result(target, "-", status="SKIPPED", reason=decision["reason"])
    log_run(run_id, target, layer, "STARTED", exec_mode=decision["exec_mode"], reason=decision["reason"],
            logic_version=spec["logic_version"], input_versions=decision["input_versions"])
    try:
        m = build(decision) or {}
    except Exception as exc:  # noqa: BLE001 — ghi lý do rồi ném lại để activity Failed
        log_run(run_id, target, layer, "FAILED", exec_mode=decision["exec_mode"], reason=decision["reason"],
                error_message=f"{type(exc).__name__}: {str(exc)[:1500]}")
        raise
    exec_mode = m.get("exec_mode", decision["exec_mode"])
    reason = m.get("reason", decision["reason"])
    out_version = table_version(target)
    if not m.get("skip_state"):
        state_put(target, spec["logic_version"], decision["input_versions"], out_version, run_id, exec_mode)
    metrics = {k: int(m.get(k) or 0) for k in ("rows_in", "rows_inserted", "rows_updated", "rows_deleted",
                                                "rows_rejected", "rows_flagged")}
    log_run(run_id, target, layer, "SUCCEEDED", exec_mode=exec_mode, reason=reason,
            logic_version=spec["logic_version"], input_versions=decision["input_versions"],
            output_version=out_version, **metrics)
    return worker_result(target, exec_mode, reason=reason, **metrics)


def gold_audit(df, run_id, logic_version):
    """Thêm cột kiểm toán chung của bảng Gold."""
    return (df.withColumn("_run_id", F.lit(run_id)).withColumn("_logic_version", F.lit(int(logic_version)))
              .withColumn("_updated_at_utc", F.current_timestamp()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

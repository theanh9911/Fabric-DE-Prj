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

# # nb_land
# Sau Copy activity: đếm, checksum, so snapshot, chuyển staging sang Landing, ghi manifest, ghi `meta.delivery_log`
# (`LANDED`) và `meta.landed_file`. Không đọc nguồn. Spec: docs/04-kien-truc.md §3.2–3.7, §9.1.
# Kết quả: `{source_id, outcome, delivery_id, rows_landed}`, `outcome` ∈ `DELIVERED`, `NO_DATA`, `UNCHANGED`.

# PARAMETERS CELL ********************

p_run_id = ""
p_source_json = ""
p_copy_rows_read = ""

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

NB_NAME = "nb_land"
import hashlib
import json

SRC = json_param(p_source_json, None, "p_source_json")
if not SRC:
    raise ValueError("p_source_json trống")
RUN_ID = p_run_id or new_id()
STEP = f"ingest.{SRC['source_id']}"


def checksum(n, s):
    return hashlib.sha256(f"{int(n)}|{int(s or 0)}".encode()).hexdigest()


def row_hash(cols):
    return F.xxhash64(*[F.coalesce(F.col(f"`{c}`").cast("string"), F.lit("<NULL>")) for c in sorted(cols)])


def last_snapshot_checksum():
    r = (spark.table("meta.v_last_snapshot_checksum")
         .where((F.col("source_system") == SRC["source_system"]) & (F.col("entity") == SRC["entity"]))
         .collect())
    return r[0]["source_checksum"] if r else None


def finish(outcome, rows=0, reason=None, delivery_id=None):
    event = {"DELIVERED": "SUCCEEDED", "NO_DATA": "NO_DATA", "UNCHANGED": "UNCHANGED"}[outcome]
    log_run(RUN_ID, STEP, "ingest", event, reason=reason, rows_in=rows)
    return {"source_id": SRC["source_id"], "outcome": outcome, "delivery_id": delivery_id, "rows_landed": rows}


def drop_staging():
    if notebookutils.fs.exists(SRC["staging_path"]):
        notebookutils.fs.rm(SRC["staging_path"], True)


def manifest_for(landing_path, fmt, files, rows, source_count, checksum_value, columns, delivered_at):
    return {"manifest_version": 1, "delivery_id": SRC["delivery_id"], "run_id": RUN_ID, "batch_id": SRC["batch_id"],
            "source_id": SRC["source_id"], "source_system": SRC["source_system"], "entity": SRC["entity"],
            "contract_version": SRC["contract_version"], "load_strategy": SRC["load_strategy"],
            "kind": SRC["kind"], "format": fmt, "window_start": SRC.get("window_start"),
            "window_end": SRC.get("window_end"), "snapshot_date": SRC.get("snapshot_date"),
            "source_files": files, "rows_landed": rows, "source_count": source_count,
            "source_checksum": checksum_value, "columns": columns, "delivered_at_utc": delivered_at}


def deliver(manifest, landing_path):
    write_manifest(landing_path, manifest)
    record_delivery(dict(manifest, landing_path=landing_path), "LANDED", RUN_ID)


def land_db():
    files = [f for f in list_files(SRC["staging_path"]) if f.name.endswith(".parquet")]
    if not files:
        drop_staging()
        return finish("NO_DATA", reason="no_staging_files")
    df = spark.read.parquet(SRC["staging_path"])
    cols = df.columns
    agg = df.select(row_hash(cols).alias("h")).agg(
        F.count(F.lit(1)).alias("n"), F.sum(F.col("h").cast("decimal(38,0)")).alias("s")).collect()[0]
    rows = int(agg["n"])
    if rows == 0:
        drop_staging()
        return finish("NO_DATA", reason="zero_rows")
    cs = checksum(rows, agg["s"])
    if SRC["kind"] == "snapshot" and cs == last_snapshot_checksum():
        drop_staging()
        return finish("UNCHANGED", rows=rows, reason="snapshot_checksum_unchanged")
    landing = landing_dir(SRC["batch_id"], SRC["source_system"], SRC["entity"], SRC["delivery_id"])
    promote_staging(SRC["delivery_id"], landing)
    copy_rows = int(p_copy_rows_read) if str(p_copy_rows_read).strip() else None
    deliver(manifest_for(landing, "parquet", [], rows, copy_rows, cs, cols, iso_utc(utc_now())), landing)
    return finish("DELIVERED", rows=rows, delivery_id=SRC["delivery_id"])


def land_file():
    expected = {(e["name"], int(e["size"])): e for e in SRC["expected_files"]}
    keep, extra = [], []
    for f in list_files(SRC["staging_path"]):
        (keep if (f.name, int(f.size)) in expected else extra).append(f)
    for f in extra:
        notebookutils.fs.rm(f.path)
    got = {(f.name, int(f.size)) for f in keep}
    missing = sorted(e["name"] for k, e in expected.items() if k not in got)
    reason = f"plan_mismatch:{','.join(missing)}" if missing else None
    if not keep:
        drop_staging()
        return finish("NO_DATA", reason=reason or "no_staging_files")
    df = read_csv_strings([f.path for f in keep])
    cols = [c for c in df.columns if c not in ("_source_file", "_row_no")]
    per_file = {r["_source_file"]: (int(r["n"]), r["s"]) for r in
                df.groupBy("_source_file").agg(F.count(F.lit(1)).alias("n"),
                                               F.sum(row_hash(cols).cast("decimal(38,0)")).alias("s")).collect()}
    files_meta = []
    for f in keep:
        e = expected[(f.name, int(f.size))]
        n, s = per_file.get(f.name, (0, 0))
        files_meta.append({"name": f.name, "size": int(f.size), "modify_time_utc": e["modify_time_utc"],
                           "rows": n, "checksum": checksum(n, s)})
    rows = sum(m["rows"] for m in files_meta)
    cs = checksum(rows, sum(int(per_file.get(m["name"], (0, 0))[1] or 0) for m in files_meta))
    landed = [{"source_system": SRC["source_system"], "entity": SRC["entity"], "file_name": m["name"],
               "file_size": m["size"], "modify_time_utc": m["modify_time_utc"], "row_count": m["rows"],
               "file_checksum": m["checksum"], "delivery_id": None, "batch_id": SRC["batch_id"]} for m in files_meta]
    if SRC["kind"] == "snapshot" and cs == last_snapshot_checksum():
        drop_staging()
        record_landed_files([dict(r, outcome="UNCHANGED") for r in landed], RUN_ID)
        return finish("UNCHANGED", rows=rows, reason="snapshot_checksum_unchanged")
    landing = landing_dir(SRC["batch_id"], SRC["source_system"], SRC["entity"], SRC["delivery_id"])
    promote_staging(SRC["delivery_id"], landing, files=[f.path for f in keep])
    header = [c.strip() for c in spark.read.option("header", True).csv(f"{landing}/{keep[0].name}").columns]
    manifest = manifest_for(landing, "csv", files_meta, rows, None, cs, header, iso_utc(utc_now()))
    deliver(manifest, landing)
    record_landed_files([dict(r, outcome="LANDED", delivery_id=SRC["delivery_id"]) for r in landed], RUN_ID)
    return finish("DELIVERED", rows=rows, reason=reason, delivery_id=SRC["delivery_id"])


try:
    RESULT = land_db() if SRC["load_strategy"].startswith("db_") else land_file()
except Exception as exc:  # noqa: BLE001
    log_run(RUN_ID, STEP, "ingest", "FAILED", error_message=f"nb_land: {type(exc).__name__}: {str(exc)[:1500]}")
    raise
exit_worker(RESULT)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

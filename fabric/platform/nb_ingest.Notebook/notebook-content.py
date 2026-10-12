# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {"name": "synapse_pyspark"},
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

p_plan_json = "{}"
p_source_workspace = "CompanyA-Source"
p_erp_item = "sqldb_erp_wholesale"
p_drop_lakehouse = "lh_retail_drop"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


%run nb_common

# CELL ********************

import json
from datetime import datetime, timezone
from pyspark.sql import functions as F

from notebookutils import fabric

current_ws = notebookutils.runtime.context["currentWorkspaceId"]
client = fabric.FabricRestClient()
workspace_rows = client.get("v1/workspaces").json().get("value", [])
source_ws = next((w for w in workspace_rows if w.get("displayName") == p_source_workspace), None)
if source_ws is None:
    raise ValueError(f"Source workspace '{p_source_workspace}' not accessible to current identity")
source_ws_id = source_ws["id"]

def resolve_sql_database(workspace_id, name):
    rows = client.get(f"v1/workspaces/{workspace_id}/sqlDatabases").json().get("value", [])
    item = next((x for x in rows if x.get("displayName") == name), None)
    if item is None:
        raise ValueError(f"SQL Database '{name}' not found in workspace {workspace_id}")
    props = client.get(f"v1/workspaces/{workspace_id}/sqlDatabases/{item['id']}").json()["properties"]
    return props["serverFqdn"].split(",")[0], props["databaseName"]

def lakehouse_id(workspace_id, name):
    rows = client.get(f"v1/workspaces/{workspace_id}/lakehouses").json().get("value", [])
    item = next((x for x in rows if x.get("displayName") == name), None)
    if item is None:
        raise ValueError(f"Lakehouse '{name}' not found in workspace {workspace_id}")
    return item["id"]

def source_lakehouse_path(name, subpath):
    item_id = lakehouse_id(source_ws_id, name)
    return f"abfss://{source_ws_id}@onelake.dfs.fabric.microsoft.com/{item_id}/{subpath}"

def source_jdbc(server, database):
    return f"jdbc:sqlserver://{server}:1433;database={database};encrypt=true;trustServerCertificate=false;loginTimeout=30"

def sql_source_frame(spec, plan):
    server, database = resolve_sql_database(source_ws_id, p_erp_item)
    cols = "*"
    where = ""
    if spec["load_strategy"] == "db_incremental":
        start = plan.get("window_start") or "1900-01-01 00:00:00"
        end = plan.get("window_end")
        where = f" WHERE [{spec['watermark_column']}] >= '{start}'"
        if end:
            where += f" AND [{spec['watermark_column']}] < '{end}'"
    query = f"(SELECT {cols} FROM {spec['source_object']}{where}) AS src"
    token = notebookutils.credentials.getToken("https://database.windows.net/")
    return (spark.read.format("jdbc").option("url", source_jdbc(server, database))
            .option("dbtable", query).option("accessToken", token)
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").load())

def file_source_frame(spec, plan):
    root = source_lakehouse_path(p_drop_lakehouse, f"Files/{spec['source_object']}")
    files = [f for f in notebookutils.fs.ls(root) if not f.isDir]
    start = plan.get("window_start")
    candidates = []
    for f in files:
        name = f.name.lower()
        if not name.endswith((".csv", ".txt")):
            continue
        modified = getattr(f, "modifyTime", None)
        if start and modified:
            try:
                start_dt = datetime.fromisoformat(str(start).replace("Z", "+00:00"))
                if start_dt.tzinfo is None:
                    start_dt = start_dt.replace(tzinfo=timezone.utc)
                modified_dt = datetime.fromtimestamp(float(modified) / 1000, timezone.utc)
                if modified_dt < start_dt:
                    continue
            except Exception:
                pass
        candidates.append(f.path)
    if not candidates:
        return None, []
    if spec["kind"] == "snapshot":
        dated = []
        for path in candidates:
            name = path.rsplit("/", 1)[-1]
            digits = "".join(ch for ch in name if ch.isdigit())
            dated.append((digits[-8:] if len(digits) >= 8 else "", path))
        dated = [x for x in dated if x[0]]
        if dated:
            latest = max(x[0] for x in dated)
            candidates = [path for day, path in dated if day == latest]
    df = (spark.read.option("header", True).option("multiLine", True).option("escape", '"')
          .option("mode", "PERMISSIVE").csv(candidates).withColumn("_source_file", F.input_file_name())
          .withColumn("_row_no", F.monotonically_increasing_id()))
    return df, candidates

plan_doc = json.loads(p_plan_json)
run_id = plan_doc["run_id"]
batch_id = plan_doc["batch_id"]
delivery_records = []
for item in plan_doc["sources"]:
    spec = item
    if spec["load_strategy"] not in ("db_incremental", "db_full_snapshot", "file_new_or_changed"):
        continue
    if spec["load_strategy"].startswith("db_"):
        df = sql_source_frame(spec, item)
        files = []
        row_count = df.count()
        if row_count == 0:
            continue
    else:
        df, files = file_source_frame(spec, item)
        if df is None:
            continue
        row_count = df.count()
        if row_count == 0:
            continue
    checksum_cols = sorted([c for c in df.columns if not c.startswith("_")])
    row_hash = F.xxhash64(*[F.col(c).cast("string") for c in checksum_cols])
    checksum_stats = df.select(row_hash.alias("_h")).agg(
        F.count(F.lit(1)).alias("n"), F.sum(F.col("_h")).alias("s")).first()
    source_checksum = hashlib.sha256(f"{checksum_stats['n']}|{checksum_stats['s']}".encode("utf-8")).hexdigest()
    if (spec["load_strategy"] in ("db_full_snapshot", "file_new_or_changed") and
            spec["kind"] == "snapshot" and item.get("last_checksum") == source_checksum and
            not item.get("force_copy", False)):
        continue
    delivery_id = str(uuid.uuid4())
    target = (f"Files/landing/{batch_id}/{spec['source_system']}/{spec['entity']}/"
              f"run={delivery_id}")
    if spec["load_strategy"].startswith("db_"):
        (df.write.mode("errorifexists").parquet(target))
        fmt = "parquet"
    else:
        # Copy original file bytes, preserving names; Spark DataFrame is used only for row count.
        notebookutils.fs.mkdirs(target)
        for source_file in files:
            name = source_file.rsplit("/", 1)[-1]
            notebookutils.fs.cp(source_file, f"{target}/{name}")
        fmt = "binary"
    manifest = {"batch_id": batch_id, "source_system": spec["source_system"],
                "entity": spec["entity"], "delivery_id": delivery_id,
                "load_strategy": spec["load_strategy"], "format": fmt,
                "window_start": item.get("window_start"), "window_end": item.get("window_end"),
                "rows_copied": row_count if fmt == "parquet" else None,
                "files_written": len(files) if fmt == "binary" else 1,
                "source_files": files, "source_checksum": source_checksum,
                "delivered_at_utc": datetime.now(timezone.utc).isoformat(),
                "watermark_value": item.get("window_end") or item.get("watermark_value"),
                "is_latest_snapshot": spec["kind"] == "snapshot"}
    # Manifest is the commit marker and is created only after all data objects are present.
    payload = json.dumps(manifest, ensure_ascii=False)
    notebookutils.fs.put(f"{target}/_manifest.json", payload, True)
    manifest["landing_path"] = target
    delivery_records.append(manifest)

notebookutils.notebook.exit(json.dumps({"run_id": run_id, "batch_id": batch_id,
                                        "deliveries": delivery_records}, default=str))

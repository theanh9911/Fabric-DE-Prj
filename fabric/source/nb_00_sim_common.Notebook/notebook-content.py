# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_00_sim_common
# Thư viện dùng chung của **Source simulator** (`%run nb_00_sim_common`).
# - **Mục đích:** config + hàm cho các notebook `nb_00_sim_*` — một chỗ duy nhất (P4).
# - **Không** chứa logic chạy; chỉ định nghĩa.
# - **Ranh giới:** chỉ ghi vào `sqldb_erp_wholesale`, `lh_retail_drop`, `lh_sim` (workspace Source).

# MARKDOWN ********************

# ## Config — nơi DUY NHẤT chứa thiết lập của simulator

# CELL ********************

import datetime as dt
import uuid
from functools import lru_cache

import sempy.fabric as fabric
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Parser mới của Spark 3: chuỗi không khớp format → NULL (thay vì lỗi upgrade)
spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")

SIM_CONFIG = {
    # Chỉ khai báo TÊN item; server/database được tra ở runtime qua Fabric REST API (không lưu trong Git)
    "erp_item": "sqldb_erp_wholesale",
    "sim_lakehouse": "lh_sim",
    "drop_lakehouse": "lh_retail_drop",
    # Virtual clock: lần chạy đầu release toàn bộ lịch sử tới ngày này, sau đó mỗi lần tiến p_days ngày.
    # 2025-12-31 → giai đoạn replay 2026-01..05 chứa đúng các kịch bản của đề (Q14 01/2026, Q15 05/2026, Q16 06/2026).
    "initial_until": "2025-12-31",
    # Data thật kết thúc ~05/2026; dòng có ngày sau mốc này là LỖI NHẬP LIỆU (năm 2027) chứ không phải tương lai
    # → coi như đã nằm sẵn trong nguồn: release ngay ở initial load để platform bắt bằng DQ future_date.
    "future_date_cutoff": "2026-12-31",
}

# Thứ tự khai báo = thứ tự release (master trước, orders sau)
ENTITIES = {
    "categories": {
        "kind": "master",
        "pk": ["category_code"],
        "sources": {"wholesale": "categories.csv"},
    },
    "customers": {
        "kind": "master",
        "pk": ["customer_code"],
        "sources": {"wholesale": "customers_wholesale.csv", "retail": "customers_retail.csv"},
    },
    "products": {
        "kind": "master",
        "pk": ["product_code"],
        "sources": {"wholesale": "products_wholesale.csv", "retail": "products_retail.csv"},
    },
    "sales_hierarchy": {
        "kind": "master",
        "pk": ["salesman_code"],
        "sources": {"wholesale": "sales_hierarchy_wholesale.csv", "retail": "sales_hierarchy_retail.csv"},
    },
    "orders": {
        "kind": "event",
        "pk": None,
        "not_null": ["order_no", "updated_at"],
        "sources": {"wholesale": "orders_wholesale.csv", "retail": "orders_retail.csv"},
    },
}

# Kiểu cột khi ghi vào ERP (hybrid: chỉ ép timestamp/date/số, còn lại giữ nguyên chuỗi)
TS_COLUMNS = {"created_at", "inserted_at", "updated_at"}
DATE_COLUMNS = {"order_date"}
DECIMAL_COLUMNS = {"quantity", "price"}

# Thử lần lượt; M/d trước d/M (format chính của ERP), d/M chỉ khớp khi ngày > 12
TS_FORMATS = [
    "yyyy-MM-dd HH:mm:ss",
    "yyyy/MM/dd HH:mm:ss",
    "M/d/yyyy H:mm:ss",
    "M/d/yyyy H:mm",
    "d/M/yyyy H:mm:ss",
    "d/M/yyyy H:mm",
    "yyyy-MM-dd",
    "M/d/yyyy",
    "d/M/yyyy",
]

# Dòng không xác định được ngày release → release ngay ở lần đầu
EPOCH = dt.date(1900, 1, 1)

# MARKDOWN ********************

# ## Paths

# CELL ********************

_WORKSPACE_ID = notebookutils.runtime.context["currentWorkspaceId"]


@lru_cache(maxsize=None)
def _lakehouse_id(lakehouse: str) -> str:
    return fabric.resolve_item_id(lakehouse, item_type="Lakehouse", workspace=_WORKSPACE_ID)


def lakehouse_path(lakehouse: str, sub_path: str) -> str:
    # OneLake không cho trộn GUID workspace + tên item → dùng GUID cho cả hai
    return f"abfss://{_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{_lakehouse_id(lakehouse)}/{sub_path}"


def sim_table(name: str) -> str:
    return lakehouse_path(SIM_CONFIG["sim_lakehouse"], f"Tables/{name}")


def seed_file(file_name: str) -> str:
    return lakehouse_path(SIM_CONFIG["sim_lakehouse"], f"Files/seed/{file_name}")


def inbound_dir(entity: str) -> str:
    return lakehouse_path(SIM_CONFIG["drop_lakehouse"], f"Files/inbound/{entity}")


def table_exists(path: str) -> bool:
    return notebookutils.fs.exists(f"{path}/_delta_log")

# MARKDOWN ********************

# ## Parse & transform

# CELL ********************

def business_columns(df: DataFrame) -> list:
    """Cột nghiệp vụ = mọi cột không bắt đầu bằng '_' (cột kỹ thuật của simulator)."""
    return [c for c in df.columns if not c.startswith("_")]


def parse_ts(col_name: str):
    return F.coalesce(*[F.expr(f"try_to_timestamp(trim(`{col_name}`), '{fmt}')") for fmt in TS_FORMATS])


def release_date_col(entity: str, columns: list):
    """Ngày dòng 'xuất hiện' ở nguồn (theo virtual clock).

    - orders: ngày của updated_at; master: ngày của max(created/inserted, updated)
    - không parse được hoặc sau future_date_cutoff (ngày sai) → EPOCH = release ở initial load
    """
    if ENTITIES[entity]["kind"] == "event":
        ts = parse_ts("updated_at")
    else:
        ts = F.greatest(*[parse_ts(c) for c in columns if c in TS_COLUMNS])
    day = F.to_date(ts)
    cutoff = F.lit(dt.date.fromisoformat(SIM_CONFIG["future_date_cutoff"]))
    return F.when(day.isNull() | (day > cutoff), F.lit(EPOCH)).otherwise(day)


def to_erp_typed(df: DataFrame) -> DataFrame:
    """Ép kiểu theo DDL hybrid của ERP. Chuỗi rỗng → NULL; giá trị bẩn khác giữ nguyên."""

    def convert(c: str):
        if c in TS_COLUMNS:
            return parse_ts(c)
        if c in DATE_COLUMNS:
            return F.to_date(parse_ts(c))
        if c in DECIMAL_COLUMNS:
            return F.expr(f"try_cast(trim(`{c}`) AS DECIMAL(18,2))")
        return F.when(F.trim(F.col(c)) == "", None).otherwise(F.col(c))

    technical = [c for c in df.columns if c.startswith("_")]
    return df.select(*[convert(c).alias(c) for c in business_columns(df)], *technical)


def split_erp_rejects(entity: str, df: DataFrame):
    """Mô phỏng ràng buộc DB (NOT NULL + PK). Trả về (ok, rejected có cột _reject_reason)."""
    spec = ENTITIES[entity]
    reason = F.lit(None).cast("string")
    for c in spec.get("not_null") or spec["pk"]:
        reason = F.when(reason.isNull() & F.col(c).isNull(), F.lit(f"NOT NULL violated: {c}")).otherwise(reason)
    df = df.withColumn("_reject_reason", reason)

    if spec["pk"]:
        # DB thật: dòng insert trước thắng, dòng trùng key sau bị từ chối
        w = Window.partitionBy(*spec["pk"]).orderBy("_row_no")
        df = (
            df.withColumn("_rn", F.row_number().over(w))
            .withColumn(
                "_reject_reason",
                F.when(F.col("_reject_reason").isNull() & (F.col("_rn") > 1), F.lit("PK violated: duplicate key"))
                .otherwise(F.col("_reject_reason")),
            )
            .drop("_rn")
        )

    ok = df.filter("_reject_reason IS NULL").drop("_reject_reason")
    rejected = df.filter("_reject_reason IS NOT NULL")
    return ok, rejected

# MARKDOWN ********************

# ## ERP (Fabric SQL Database) — JDBC + Entra token

# CELL ********************

@lru_cache(maxsize=1)
def _erp_connection() -> tuple:
    """Tra (server, database) của SQL DB theo tên item trong workspace hiện tại."""
    client = fabric.FabricRestClient()
    items = client.get(f"v1/workspaces/{_WORKSPACE_ID}/sqlDatabases").json()["value"]
    item = next((i for i in items if i["displayName"] == SIM_CONFIG["erp_item"]), None)
    if item is None:
        raise ValueError(f"SQL database '{SIM_CONFIG['erp_item']}' not found in this workspace")
    props = client.get(f"v1/workspaces/{_WORKSPACE_ID}/sqlDatabases/{item['id']}").json()["properties"]
    server = props["serverFqdn"].split(",")[0]   # có thể kèm ",1433"
    return server, props["databaseName"]


def _erp_jdbc_url() -> str:
    server, database = _erp_connection()
    return (
        f"jdbc:sqlserver://{server}:1433;database={database};"
        "encrypt=true;trustServerCertificate=false;loginTimeout=30"
    )


def _erp_token() -> str:
    return notebookutils.credentials.getToken("https://database.windows.net/")


def erp_write_stage(df: DataFrame, entity: str) -> None:
    """Ghi đè sim.stg_<entity> (TRUNCATE giữ nguyên kiểu cột đã khai báo trong DDL)."""
    (
        df.write.format("jdbc")
        .mode("overwrite")
        .option("url", _erp_jdbc_url())
        .option("accessToken", _erp_token())
        .option("dbtable", f"sim.stg_{entity}")
        .option("truncate", "true")
        .option("batchsize", 10000)
        .save()
    )


def erp_exec(sql: str) -> None:
    """Chạy 1 khối T-SQL trong 1 transaction (lỗi → rollback toàn bộ)."""
    jvm = spark.sparkContext._gateway.jvm
    props = jvm.java.util.Properties()
    props.setProperty("accessToken", _erp_token())
    conn = jvm.java.sql.DriverManager.getConnection(_erp_jdbc_url(), props)
    try:
        conn.setAutoCommit(False)
        conn.createStatement().execute(sql)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def erp_merge_sql(entity: str, cols: list, pk: list) -> str:
    on = " AND ".join(f"t.[{c}] = s.[{c}]" for c in pk)
    updates = ", ".join(f"t.[{c}] = s.[{c}]" for c in cols if c not in pk)
    insert_cols = ", ".join(f"[{c}]" for c in cols)
    insert_vals = ", ".join(f"s.[{c}]" for c in cols)
    return (
        f"MERGE dbo.[{entity}] AS t USING sim.[stg_{entity}] AS s ON {on} "
        f"WHEN MATCHED THEN UPDATE SET {updates} "
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});"
    )


def erp_replace_window_sql(entity: str, cols: list, window_from: dt.date, window_to: dt.date,
                           is_initial_load: bool) -> str:
    """Xoá rồi chèn lại đúng phần dữ liệu của cửa sổ → chạy lại cùng cửa sổ không nhân đôi dòng.

    Initial load xoá toàn bảng (cửa sổ đầu gồm cả dòng ngày sai/không parse được, nằm ngoài khoảng updated_at).
    """
    col_list = ", ".join(f"[{c}]" for c in cols)
    to_exclusive = window_to + dt.timedelta(days=1)
    delete_where = "" if is_initial_load else f" WHERE updated_at >= '{window_from}' AND updated_at < '{to_exclusive}'"
    return (
        f"DELETE FROM dbo.[{entity}]{delete_where}; "
        f"INSERT INTO dbo.[{entity}] ({col_list}) SELECT {col_list} FROM sim.[stg_{entity}];"
    )

# MARKDOWN ********************

# ## Retail drop — ghi 1 file CSV đúng tên

# CELL ********************

def write_single_csv(df: DataFrame, target_file: str, order_by: str = "_row_no") -> None:
    """Spark ghi ra thư mục → gom 1 partition, giữ thứ tự gốc, đổi tên part-file thành target_file."""
    tmp_dir = f"{target_file}.__tmp"
    (
        df.repartition(1)
        .sortWithinPartitions(order_by)
        .select(*business_columns(df))
        .write.mode("overwrite")
        .option("header", True)
        .option("escape", '"')                      # RFC 4180: " trong giá trị → "" (mặc định Spark là \")
        .option("emptyValue", "")
        .option("ignoreLeadingWhiteSpace", False)   # giữ nguyên khoảng trắng "bẩn" của file gốc
        .option("ignoreTrailingWhiteSpace", False)
        .csv(tmp_dir)
    )
    part = next(f.path for f in notebookutils.fs.ls(tmp_dir) if f.name.startswith("part-"))
    notebookutils.fs.mv(part, target_file, True, True)
    notebookutils.fs.rm(tmp_dir, True)

# MARKDOWN ********************

# ## State & log (lh_sim)

# CELL ********************

def get_released_until():
    path = sim_table("sim_state")
    if not table_exists(path):
        return None
    row = (
        spark.read.format("delta").load(path)
        .filter("state_key = 'released_until'")
        .select("state_value")
        .first()
    )
    return dt.date.fromisoformat(row[0]) if row else None


def set_released_until(value: dt.date, sim_run_id: str) -> None:
    (
        spark.createDataFrame(
            [("released_until", value.isoformat(), sim_run_id)],
            "state_key string, state_value string, sim_run_id string",
        )
        .withColumn("updated_at", F.current_timestamp())
        .write.format("delta").mode("overwrite").save(sim_table("sim_state"))
    )


def write_window_log(df: DataFrame, table: str, window_to: dt.date) -> None:
    """Log theo cửa sổ: chạy lại cùng window_to → ghi đè đúng phần đó (idempotent)."""
    path = sim_table(table)
    writer = df.write.format("delta")
    if table_exists(path):
        writer.mode("overwrite").option("replaceWhere", f"window_to = DATE'{window_to}'").save(path)
    else:
        writer.mode("overwrite").save(path)


def reject_log_df(rejected: DataFrame, sim_run_id: str, source: str, entity: str,
                  window_from: dt.date, window_to: dt.date) -> DataFrame:
    return rejected.select(
        F.lit(sim_run_id).alias("sim_run_id"),
        F.lit(source).alias("source_system"),
        F.lit(entity).alias("entity"),
        F.lit(window_from).cast("date").alias("window_from"),
        F.lit(window_to).cast("date").alias("window_to"),
        F.col("_reject_reason").alias("reason"),
        F.to_json(F.struct(*business_columns(rejected))).alias("row_json"),
        F.current_timestamp().alias("logged_at"),
    )

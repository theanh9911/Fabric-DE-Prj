# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a2210167-f7b0-4566-a017-886910bd735c",
# META       "default_lakehouse_name": "lh_sim",
# META       "default_lakehouse_workspace_id": "a2127e64-3558-425b-9549-b371fd8bce0c",
# META       "known_lakehouses": [
# META         {
# META           "id": "a2210167-f7b0-4566-a017-886910bd735c"
# META         },
# META         {
# META           "id": "9fb95c8f-fbfa-4ead-91f0-ab104ab02a2a"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_00_sim_inject
# Đóng vai team nguồn gây **sự cố có kiểm soát** để diễn tập lặp lại được (docs/02-nguon.md §3, docs/07 §5).
# Chạy sau `nb_00_sim_daily` của ngày giả lập hiện tại và trước platform. Ghi mỗi lần tiêm một dòng vào
# `lh_sim.sim_inject_log`. Cần `p_confirm = "INJECT"`.
#
# | `p_scenario` | Hành động | Mặc định `p_entity` |
# |---|---|---|
# | `file_resend` | ghi đè file orders của ngày hiện tại cùng tên, sửa `quantity` của dòng `_row_no` nhỏ nhất | `retail.orders` |
# | `duplicate_file` | copy file orders của ngày hiện tại sang `orders_YYYYMMDD_copy.csv` | `retail.orders` |
# | `missing_file` | xoá file của ngày hiện tại (đã ghi log nội dung) | `retail.orders` |
# | `partial_file` | ghi lại file với nửa số dòng đầu; lần chạy thứ hai cùng tham số ghi đủ | `retail.orders` |
# | `schema_change` | thêm cột `discount_amount` (giá trị `0`) vào file | `retail.orders` |
# | `master_delete` | xoá mã nhỏ nhất khỏi snapshot mới nhất (file: ghi snapshot ngày hiện tại; ERP: `DELETE`) | `retail.products` |
# | `org_change` | đổi `salesmanager_code` của salesman nhỏ nhất có quản lý sang quản lý khác | `wholesale.sales_hierarchy` |
# | `bad_rows` | thêm 3 dòng bẩn vào file orders ngày hiện tại | `retail.orders` |

# PARAMETERS CELL ********************

p_scenario = ""
p_entity = ""
p_confirm = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_00_sim_common

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

SCENARIOS = {"file_resend": "retail.orders", "duplicate_file": "retail.orders", "missing_file": "retail.orders",
             "partial_file": "retail.orders", "schema_change": "retail.orders", "master_delete": "retail.products",
             "org_change": "wholesale.sales_hierarchy", "bad_rows": "retail.orders"}
if p_confirm != "INJECT":
    print('p_confirm khác "INJECT": không làm gì.')
    notebookutils.notebook.exit("NOOP")
if p_scenario not in SCENARIOS:
    raise ValueError(f"p_scenario không hợp lệ: {p_scenario!r}; chọn một trong {sorted(SCENARIOS)}")
ENTITY = p_entity or SCENARIOS[p_scenario]
SOURCE, ENT = ENTITY.split(".", 1)
SIM_DATE = get_released_until()
if SIM_DATE is None:
    raise RuntimeError("simulator chưa chạy (chưa có released_until): chạy nb_00_sim_daily trước")
STAMP = f"{SIM_DATE:%Y%m%d}"
FOLDER = inbound_dir(SOURCE, ENT)
DETAIL = {}


def read_csv(path):
    df = (spark.read.option("header", True).option("multiLine", True).option("quote", '"').option("escape", '"')
          .option("inferSchema", False).csv(path))
    return df.withColumn("_row_no", F.monotonically_increasing_id())


def day_file(entity_name=None):
    name = f"{entity_name or ENT}_{STAMP}.csv"
    path = f"{FOLDER}/{name}"
    if not notebookutils.fs.exists(path):
        raise RuntimeError(f"không có file của ngày {SIM_DATE}: {path}")
    return name, path


def newest_snapshot():
    files = [f for f in notebookutils.fs.ls(FOLDER) if f.name.startswith(f"{ENT}_") and f.name.endswith(".csv")]
    if not files:
        raise RuntimeError(f"không có snapshot nào ở {FOLDER}")
    return max(files, key=lambda f: f.name)


def backup_path(name):
    return lakehouse_path(SIM_CONFIG["sim_lakehouse"], f"Files/inject_backup/{name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- kịch bản trên file orders của ngày hiện tại ----------------------------------------------------------

if p_scenario == "file_resend":
    name, path = day_file()
    df = read_csv(path)
    target = df.orderBy("_row_no").first()
    new_qty = "999"
    df2 = df.withColumn("quantity", F.when(F.col("_row_no") == target["_row_no"], F.lit(new_qty))
                        .otherwise(F.col("quantity")))
    write_single_csv(df2, path)
    DETAIL = {"file": name, "order_no": target["order_no"], "old_quantity": target["quantity"],
              "new_quantity": new_qty}

elif p_scenario == "duplicate_file":
    name, path = day_file()
    copy_name = f"{ENT}_{STAMP}_copy.csv"
    notebookutils.fs.cp(path, f"{FOLDER}/{copy_name}", False)
    DETAIL = {"file": name, "copy": copy_name}

elif p_scenario == "missing_file":
    name, path = day_file()
    DETAIL = {"file": name, "rows": read_csv(path).count()}
    notebookutils.fs.rm(path)

elif p_scenario == "partial_file":
    name, path = day_file()
    bak = backup_path(name)
    if notebookutils.fs.exists(bak):
        notebookutils.fs.mv(bak, path, True, True)
        DETAIL = {"file": name, "phase": "restore_full"}
    else:
        notebookutils.fs.cp(path, bak, True)
        df = read_csv(path)
        total = df.count()
        half = df.orderBy("_row_no").limit(max(total // 2, 1))
        write_single_csv(half, path)
        DETAIL = {"file": name, "phase": "write_half", "rows_total": total, "rows_written": max(total // 2, 1)}

elif p_scenario == "schema_change":
    name, path = day_file()
    df = read_csv(path).withColumn("discount_amount", F.lit("0"))
    write_single_csv(df, path)
    DETAIL = {"file": name, "added_column": "discount_amount"}

elif p_scenario == "bad_rows":
    name, path = day_file()
    df = read_csv(path)
    base = df.orderBy("_row_no").first().asDict()
    n = df.count()
    bad = []
    for i, change in enumerate(({"updated_at": ""}, {"order_status": "Shiped"}, {"quantity": "abc"}), start=1):
        r = dict(base)
        r.update(change)
        r["order_no"] = f"INJ{STAMP}{i}"
        r["_row_no"] = n + i
        bad.append(r)
    extra = spark.createDataFrame([tuple(r[c] for c in df.columns) for r in bad], df.schema)
    write_single_csv(df.unionByName(extra), path)
    DETAIL = {"file": name, "rows_added": 3, "order_nos": [r["order_no"] for r in bad]}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---- kịch bản master -------------------------------------------------------------------------------------

if p_scenario == "master_delete":
    key = ENTITIES[ENT]["pk"][0]
    if SOURCES[SOURCE] == "file":
        snap = newest_snapshot()
        df = read_csv(f"{FOLDER}/{snap.name}")
        victim = df.agg(F.min(key)).first()[0]
        out = df.filter(F.col(key) != victim)
        write_single_csv(out, f"{FOLDER}/{ENT}_{STAMP}.csv")
        DETAIL = {"from": snap.name, "wrote": f"{ENT}_{STAMP}.csv", "deleted_key": victim}
    else:
        seed = spark.read.format("delta").load(sim_table(f"seed_{SOURCE}_{ENT}"))
        victim = seed.filter(F.col("_release_date") <= F.lit(SIM_DATE)).agg(F.min(key)).first()[0]
        erp_exec(f"DELETE FROM dbo.[{ENT}] WHERE [{key}] = N'{victim}'")
        DETAIL = {"erp_table": f"dbo.{ENT}", "deleted_key": victim}

elif p_scenario == "org_change":
    if SOURCES[SOURCE] != "erp" or ENT != "sales_hierarchy":
        raise ValueError("org_change chỉ hỗ trợ wholesale.sales_hierarchy (ERP)")
    seed = spark.read.format("delta").load(sim_table(f"seed_{SOURCE}_{ENT}")).filter(
        F.col("_release_date") <= F.lit(SIM_DATE))
    with_mgr = seed.filter(F.col("salesmanager_code").isNotNull() & (F.trim("salesmanager_code") != ""))
    victim = with_mgr.orderBy("salesman_code").first()
    managers = sorted(r[0] for r in with_mgr.select("salesmanager_code").distinct().collect())
    others = [m for m in managers if m != victim["salesmanager_code"] and m != victim["salesman_code"]]
    if not others:
        raise RuntimeError("không có quản lý khác để đổi sang")
    new_mgr = others[0]
    erp_exec(f"UPDATE dbo.[sales_hierarchy] SET [salesmanager_code] = N'{new_mgr}', "
             f"[updated_at] = '{SIM_DATE} 12:00:00' WHERE [salesman_code] = N'{victim['salesman_code']}'")
    DETAIL = {"salesman_code": victim["salesman_code"], "old_manager": victim["salesmanager_code"],
              "new_manager": new_mgr}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

log_df = spark.createDataFrame(
    [(SIM_DATE, p_scenario, ENTITY, json.dumps(DETAIL, ensure_ascii=False, default=str))],
    "sim_date date, scenario string, entity string, detail_json string",
).withColumn("injected_at", F.current_timestamp())
log_df.write.format("delta").mode("append").save(sim_table("sim_inject_log"))
print(json.dumps({"scenario": p_scenario, "entity": ENTITY, "sim_date": str(SIM_DATE), "detail": DETAIL},
                 ensure_ascii=False, indent=2, default=str))
notebookutils.notebook.exit(json.dumps({"scenario": p_scenario, "entity": ENTITY, "sim_date": str(SIM_DATE)}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

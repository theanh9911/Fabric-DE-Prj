# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_00_sim_daily
#
# - **Mục đích:** "hệ thống nguồn" phát sinh dữ liệu — release mọi dòng seed có `_release_date` trong cửa sổ
#   `(released_until, p_sim_date]`.
# - **Wholesale → `sqldb_erp_wholesale`:** master MERGE, orders xoá-rồi-chèn theo cửa sổ `updated_at`.
#   Dòng vi phạm NOT NULL / PK bị "DB từ chối" → `lh_sim.sim_reject_log`.
# - **Retail → `lh_retail_drop/Files/inbound/<entity>/`:**
#   orders 1 file/ngày (`orders_YYYYMMDD.csv`), cửa sổ dài (initial load) → 1 file `orders_history_until_YYYYMMDD.csv`;
#   master có thay đổi → file snapshot `<entity>_YYYYMMDD.csv`.
# - **Lần chạy đầu** = initial load toàn bộ lịch sử tới `p_sim_date`. Dòng có ngày tương lai được giữ lại,
#   tự release khi tới ngày.
# - **Bù ngày:** lỡ N ngày → lần sau release đủ N ngày. **Chạy lại** cùng ngày → không release gì thêm.
# - **Lịch:** hằng ngày 05:00 (trước platform 06:00).

# PARAMETERS CELL ********************

p_sim_date = ""   # 'YYYY-MM-DD' — release tới hết ngày này. Rỗng = hôm qua (UTC)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %run nb_00_sim_common

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sim_date = (
    dt.date.fromisoformat(p_sim_date)
    if p_sim_date
    else dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=1)
)
released_until = get_released_until()
is_initial_load = released_until is None
window_from = EPOCH if is_initial_load else released_until + dt.timedelta(days=1)
window_to = sim_date

if window_to < window_from:
    notebookutils.notebook.exit(f"NOTHING_TO_RELEASE: released_until={released_until}, p_sim_date={sim_date}")

sim_run_id = f"sim_{window_to:%Y%m%d}_{uuid.uuid4().hex[:8]}"
print(f"{sim_run_id}: release window [{window_from} → {window_to}]")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def release_wholesale(entity: str, batch: DataFrame):
    """Ghi batch vào ERP. Trả về (rows_released, rejected_df, target)."""
    spec = ENTITIES[entity]
    ok, rejected = split_erp_rejects(entity, to_erp_typed(batch))
    cols = business_columns(ok)
    ok = ok.select(*cols).cache()
    n_ok = ok.count()

    if spec["kind"] == "master":
        if n_ok:
            erp_write_stage(ok, entity)
            erp_exec(erp_merge_sql(entity, cols, spec["pk"]))
    else:
        # Luôn chạy (kể cả 0 dòng) để cửa sổ trong ERP khớp đúng seed → idempotent
        erp_write_stage(ok, entity)
        erp_exec(erp_replace_window_sql(entity, cols, window_from, window_to))

    ok.unpersist()
    return n_ok, rejected, f"sqldb_erp_wholesale.dbo.{entity}"


def release_retail(entity: str, seed: DataFrame, batch: DataFrame):
    """Thả file CSV vào inbound, giữ nguyên dữ liệu gốc. Trả về (rows_released, None, target)."""
    folder = inbound_dir(entity)

    if ENTITIES[entity]["kind"] == "master":
        if batch.isEmpty():
            return 0, None, folder
        # Hệ thống file gửi snapshot đầy đủ của master mỗi khi có thay đổi
        snapshot = seed.filter(F.col("_release_date") <= F.lit(window_to))
        write_single_csv(snapshot, f"{folder}/{entity}_{window_to:%Y%m%d}.csv")
        return snapshot.count(), None, folder

    # Chỉ initial load mới gửi 1 file lịch sử; bù ngày lỡ vẫn gửi đủ từng file ngày như hệ thống thật
    if is_initial_load:
        write_single_csv(batch, f"{folder}/{entity}_history_until_{window_to:%Y%m%d}.csv")
    else:
        days = sorted(r[0] for r in batch.select("_release_date").distinct().collect())
        for day in days:
            write_single_csv(batch.filter(F.col("_release_date") == F.lit(day)), f"{folder}/{entity}_{day:%Y%m%d}.csv")
    return batch.count(), None, folder

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

release_rows = []
reject_dfs = []

for entity, spec in ENTITIES.items():
    for source in spec["sources"]:
        seed = spark.read.format("delta").load(sim_table(f"seed_{source}_{entity}"))
        batch = seed.filter(F.col("_release_date").between(F.lit(window_from), F.lit(window_to)))

        if source == "wholesale":
            n_rows, rejected, target = release_wholesale(entity, batch)
        else:
            n_rows, rejected, target = release_retail(entity, seed, batch)

        n_rejected = 0
        if rejected is not None:
            n_rejected = rejected.count()
            if n_rejected:
                reject_dfs.append(reject_log_df(rejected, sim_run_id, source, entity, window_from, window_to))

        release_rows.append((sim_run_id, source, entity, window_from, window_to, n_rows, n_rejected, target))
        print(f"  {source:<9} {entity:<16} released={n_rows:>7}  rejected={n_rejected}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

release_log = spark.createDataFrame(
    release_rows,
    "sim_run_id string, source_system string, entity string, window_from date, window_to date, "
    "rows_released long, rows_rejected long, target string",
).withColumn("logged_at", F.current_timestamp())
write_window_log(release_log, "sim_release_log", window_to)

if reject_dfs:
    reject_log = reject_dfs[0]
    for df in reject_dfs[1:]:
        reject_log = reject_log.unionByName(df)
    write_window_log(reject_log, "sim_reject_log", window_to)

# State chỉ tiến khi mọi bước phía trên thành công
set_released_until(window_to, sim_run_id)

display(release_log)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

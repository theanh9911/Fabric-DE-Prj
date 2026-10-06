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
# # - **Mục đích:** "hệ thống nguồn" phát sinh dữ liệu theo **virtual clock** — release mọi dòng seed có
#   `_release_date` trong cửa sổ `(released_until, released_until + p_days]`.
# - **Virtual clock:** ngày giả lập lưu trong `lh_sim.sim_state`, độc lập với ngày thật. Data giữ nguyên ngày gốc.
# - **Wholesale → `sqldb_erp_wholesale`:** master MERGE, orders xoá-rồi-chèn theo cửa sổ `updated_at`.
#   Dòng vi phạm NOT NULL / PK bị "DB từ chối" → `lh_sim.sim_reject_log`.
# - **Retail → `lh_retail_drop/Files/inbound/<entity>/`:**
#   orders 1 file/ngày (`orders_YYYYMMDD.csv`), cửa sổ dài (initial load) → 1 file `orders_history_until_YYYYMMDD.csv`;
#   master có thay đổi → file snapshot `<entity>_YYYYMMDD.csv`.
# - **Lần chạy đầu** = initial load toàn bộ lịch sử tới `SIM_CONFIG.initial_until` (kèm dòng ngày sai/không parse được).
# - **Các lần sau:** tiến `p_days` ngày giả lập (mặc định 1). `p_sim_date` = nhảy tới đúng ngày (tua nhanh / drill).
# - **Chạy lại sau lỗi:** state chưa tiến → cùng cửa sổ, ghi lại không nhân đôi. State chỉ tiến khi mọi bước thành công.
# - **Exit value:** ngày giả lập mới (`YYYY-MM-DD`) → pipeline truyền làm `p_load_date` cho Platform.
# - **Lịch:** hằng ngày 05:00 (1 ngày thật = 1 ngày giả lập), hoặc `pl_sim_drive` để tua nhiều ngày.

# PARAMETERS CELL ********************

p_days = 1        # số ngày giả lập tiến thêm mỗi lần chạy
p_sim_date = ""   # 'YYYY-MM-DD' — (tuỳ chọn) nhảy tới đúng ngày này, bỏ qua p_days

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_00_sim_common%run nb_00_sim_common

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

released_until = get_released_until()
is_initial_load = released_until is None

if is_initial_load:
    window_from = EPOCH
    window_to = dt.date.fromisoformat(p_sim_date or SIM_CONFIG["initial_until"])
else:
    window_from = released_until + dt.timedelta(days=1)
    window_to = (
        dt.date.fromisoformat(p_sim_date) if p_sim_date
        else released_until + dt.timedelta(days=int(p_days))
    )

if window_to < window_from:
    # Không lùi đồng hồ được — muốn làm lại từ đầu thì chạy nb_00_sim_reset
    notebookutils.notebook.exit(str(released_until))

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
        erp_exec(erp_replace_window_sql(entity, cols, window_from, window_to, is_initial_load))

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

last_seed_date = max(
    spark.read.format("delta").load(sim_table(f"seed_{source}_orders")).agg(F.max("_release_date")).first()[0]
    for source in ENTITIES["orders"]["sources"]
)
if window_to >= last_seed_date:
    print(f"⚠️ Seed orders đã release hết (ngày cuối {last_seed_date}). Các ngày sau cần chế độ generate.")

# Ngày giả lập mới → pipeline dùng làm p_load_date cho Platform
notebookutils.notebook.exit(str(window_to))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

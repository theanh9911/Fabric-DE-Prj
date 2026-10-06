# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # nb_00_sim_setup
# - **Mục đích:** chuẩn bị "kho" dữ liệu cho simulator — đọc 9 CSV gốc, gắn ngày release cho từng dòng.
# - **Input:** `lh_sim/Files/seed/*.csv` (upload tay 1 lần từ đề bài).
# - **Output:** `lh_sim/Tables/seed_<source>_<entity>` — giữ nguyên mọi cột dạng chuỗi + `_row_no`, `_release_date`.
# - **Chạy:** 1 lần (hoặc khi đổi seed). Idempotent: ghi đè toàn bộ bảng seed, **không** đụng state/ERP/inbound.

# CELL ********************

%run nb_00_sim_common%run nb_00_sim_common

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary = []

for entity, spec in ENTITIES.items():
    for source, file_name in spec["sources"].items():
        raw = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)        # mọi cột là chuỗi — giữ nguyên data gốc
            .option("multiLine", False)
            .option("escape", '"')
            .csv(seed_file(file_name))
        )
        seed = (
            raw.withColumn("_row_no", F.monotonically_increasing_id())
            .withColumn("_release_date", release_date_col(entity, raw.columns))
        )
        (
            seed.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(sim_table(f"seed_{source}_{entity}"))
        )

        stats = seed.agg(
            F.count("*").alias("rows"),
            F.min("_release_date").alias("first_release"),
            F.max("_release_date").alias("last_release"),
            F.sum(F.when(F.col("_release_date") == F.lit(EPOCH), 1).otherwise(0)).alias("no_date_rows"),
        ).first()
        summary.append((source, entity, file_name, *stats))

display(spark.createDataFrame(
    summary,
    "source string, entity string, file string, rows long, first_release date, last_release date, no_date_rows long",
))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

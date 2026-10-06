# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# MAGIC %%configure
# MAGIC { "defaultLakehouse": { "name": "lh_platform" } }


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # nb_setup_ddl
# - **Mục đích:** tạo schema theo layer (`brz`, `slv`, `gld`, `meta`) và các bảng `meta.*` trong `lh_platform`.
# - **Bảng layer** (`brz.*`, `slv.*`, `gld.*`) do notebook của từng bước tạo — không khai báo ở đây.
# - **Idempotent:** chỉ `CREATE … IF NOT EXISTS` → chạy lại bao nhiêu lần cũng được, không đụng dữ liệu.
# - **Đổi schema:** thêm cell mới ở cuối (`ALTER TABLE … ADD COLUMNS …`), không sửa cell cũ → môi trường nào cũng bắt kịp bằng cách chạy lại.
# - **Chạy:** sau khi tạo workspace / Update từ Git, trước `nb_setup_config`.

# MARKDOWN ********************

# ## Schema

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS brz;
# MAGIC CREATE SCHEMA IF NOT EXISTS slv;
# MAGIC CREATE SCHEMA IF NOT EXISTS gld;
# MAGIC CREATE SCHEMA IF NOT EXISTS meta;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Config `cfg_*` — nội dung do `nb_setup_config` ghi (nguồn sự thật: Git)

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS meta.cfg_source_entity (
# MAGIC     source_system    STRING  COMMENT 'wholesale / retail / reference',
# MAGIC     entity           STRING  COMMENT 'customers, products, sales_hierarchy, orders, categories',
# MAGIC     source_type      STRING  COMMENT 'db (ERP SQL DB) / file (drop zone)',
# MAGIC     source_object    STRING  COMMENT 'db: dbo.<table> · file: thư mục trong lh_retail_drop (Files/…)',
# MAGIC     load_type        STRING  COMMENT 'incremental / full_snapshot',
# MAGIC     watermark_type   STRING  COMMENT 'column / file_modified',
# MAGIC     watermark_column STRING  COMMENT 'cột watermark khi watermark_type = column',
# MAGIC     lookback_days    INT     COMMENT 'lùi watermark để bắt dòng commit trễ',
# MAGIC     source_timezone  STRING  COMMENT 'múi giờ của timestamp nguồn',
# MAGIC     business_keys    STRING  COMMENT 'khoá nghiệp vụ, phân tách bằng dấu phẩy',
# MAGIC     load_order       INT     COMMENT 'nhỏ chạy trước (master trước orders)',
# MAGIC     is_active        BOOLEAN
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS meta.cfg_pipeline_step (
# MAGIC     layer         STRING         COMMENT 'brz / slv / gld',
# MAGIC     step          STRING         COMMENT 'tên bước, duy nhất trong layer',
# MAGIC     notebook      STRING         COMMENT 'notebook chạy bước này',
# MAGIC     target_table  STRING         COMMENT 'bảng đích → đếm dòng từ Delta history, chạy DQ',
# MAGIC     depends_on    ARRAY<STRING>  COMMENT 'các step cùng layer phải xong trước',
# MAGIC     timeout_sec   INT,
# MAGIC     is_active     BOOLEAN
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS meta.cfg_dq_rule (
# MAGIC     rule_id         STRING,
# MAGIC     layer           STRING,
# MAGIC     table_name      STRING   COMMENT 'schema.table',
# MAGIC     check_type      STRING   COMMENT 'not_null / unique / range / fk / freshness / custom',
# MAGIC     sql_expression  STRING   COMMENT 'điều kiện của dòng LỖI (WHERE …)',
# MAGIC     severity        STRING   COMMENT 'critical / warning',
# MAGIC     action          STRING   COMMENT 'stop / alert / quarantine / flag',
# MAGIC     description     STRING,
# MAGIC     is_active       BOOLEAN
# MAGIC ) USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Luật nghiệp vụ `ref_*` — nội dung do `nb_setup_config` ghi

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS meta.ref_order_status (
# MAGIC     status               STRING   COMMENT 'giá trị chuẩn',
# MAGIC     sequence             INT      COMMENT 'thứ tự trong vòng đời đơn',
# MAGIC     is_final             BOOLEAN,
# MAGIC     is_sales_recognized  BOOLEAN  COMMENT 'true = tính doanh thu'
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS meta.ref_value_mapping (
# MAGIC     domain          STRING  COMMENT 'gender / position / order_status / brand / country …',
# MAGIC     source_value    STRING  COMMENT 'upper(trim(giá trị nguồn))',
# MAGIC     standard_value  STRING
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS meta.ref_holiday_vn (
# MAGIC     holiday_date  DATE,
# MAGIC     holiday_name  STRING
# MAGIC ) USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Runtime `state_*` — pipeline ghi khi chạy

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS meta.state_watermark (
# MAGIC     step             STRING     COMMENT 'ingest / …',
# MAGIC     source_system    STRING,
# MAGIC     entity           STRING,
# MAGIC     watermark_value  STRING     COMMENT 'timestamp UTC dạng ISO',
# MAGIC     run_id           STRING,
# MAGIC     updated_at       TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- append-only: trạng thái hiện tại của 1 file = sự kiện mới nhất
# MAGIC CREATE TABLE IF NOT EXISTS meta.state_file_manifest (
# MAGIC     file_id        STRING     COMMENT 'hash(path)',
# MAGIC     source_system  STRING,
# MAGIC     entity         STRING,
# MAGIC     path           STRING,
# MAGIC     size_bytes     BIGINT,
# MAGIC     modified_at    TIMESTAMP,
# MAGIC     checksum       STRING,
# MAGIC     batch_id       STRING,
# MAGIC     status         STRING     COMMENT 'LANDED / LOADED / DUPLICATE / FAILED',
# MAGIC     event_at       TIMESTAMP
# MAGIC ) USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Log, DQ, recon — append-only, runner ghi

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS meta.log_pipeline_run (
# MAGIC     run_id     STRING,
# MAGIC     pipeline   STRING,
# MAGIC     load_date  DATE       COMMENT 'ngày logic (virtual clock)',
# MAGIC     status     STRING     COMMENT 'STARTED / SUCCESS / FAILED',
# MAGIC     message    STRING,
# MAGIC     event_at   TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS meta.log_task_run (
# MAGIC     task_id        STRING,
# MAGIC     run_id         STRING,
# MAGIC     layer          STRING,
# MAGIC     step           STRING,
# MAGIC     status         STRING     COMMENT 'RUNNING / SUCCESS / FAILED',
# MAGIC     rows_inserted  BIGINT,
# MAGIC     rows_updated   BIGINT,
# MAGIC     rows_deleted   BIGINT,
# MAGIC     delta_version  BIGINT     COMMENT 'version bảng đích sau bước → RESTORE khi cần',
# MAGIC     error          STRING,
# MAGIC     started_at     TIMESTAMP,
# MAGIC     ended_at       TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS meta.dq_result_log (
# MAGIC     run_id        STRING,
# MAGIC     rule_id       STRING,
# MAGIC     table_name    STRING,
# MAGIC     checked_rows  BIGINT,
# MAGIC     failed_rows   BIGINT,
# MAGIC     status        STRING     COMMENT 'PASS / FAIL',
# MAGIC     event_at      TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS meta.recon_result (
# MAGIC     run_id        STRING,
# MAGIC     check_name    STRING,
# MAGIC     left_count    BIGINT,
# MAGIC     right_count   BIGINT,
# MAGIC     left_amount   DECIMAL(38,2),
# MAGIC     right_amount  DECIMAL(38,2),
# MAGIC     diff_count    BIGINT,
# MAGIC     diff_amount   DECIMAL(38,2),
# MAGIC     status        STRING     COMMENT 'PASS / FAIL',
# MAGIC     event_at      TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS meta.schema_registry (
# MAGIC     table_name   STRING,
# MAGIC     column_name  STRING,
# MAGIC     data_type    STRING,
# MAGIC     status       STRING     COMMENT 'DETECTED / APPROVED',
# MAGIC     run_id       STRING,
# MAGIC     event_at     TIMESTAMP
# MAGIC ) USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Kiểm tra

# CELL ********************

# MAGIC %%sql
# MAGIC SHOW TABLES IN meta;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

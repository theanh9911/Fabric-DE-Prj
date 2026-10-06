-- V001 — schema theo layer + bảng meta nền tảng (config / state / log).
-- Quy tắc migration: chỉ THÊM file mới, không sửa file đã áp dụng (checksum bị kiểm tra);
-- mọi câu lệnh idempotent (IF NOT EXISTS) vì Spark DDL không có transaction.

CREATE SCHEMA IF NOT EXISTS brz;
CREATE SCHEMA IF NOT EXISTS slv;
CREATE SCHEMA IF NOT EXISTS gld;
CREATE SCHEMA IF NOT EXISTS meta;

-- Config: đồng bộ từ src/companya_de/config/source_entity.yml (nb_setup_config) — không sửa tay
CREATE TABLE IF NOT EXISTS meta.cfg_source_entity (
    source_system     STRING  NOT NULL,
    entity            STRING  NOT NULL,
    source_type       STRING  NOT NULL,
    source_object     STRING  NOT NULL,
    load_type         STRING  NOT NULL,
    watermark_type    STRING,
    watermark_column  STRING,
    lookback_days     INT     NOT NULL,
    source_timezone   STRING  NOT NULL,
    business_keys     ARRAY<STRING> NOT NULL,
    bronze_table      STRING  NOT NULL,
    is_active         BOOLEAN NOT NULL,
    load_order        INT     NOT NULL,
    synced_at         TIMESTAMP NOT NULL
) USING DELTA
COMMENT 'Nguồn dữ liệu Platform ingest (config as code)';

-- State: watermark của từng bước ingest; chỉ tiến khi bước thành công
CREATE TABLE IF NOT EXISTS meta.state_watermark (
    pipeline_step     STRING  NOT NULL,
    source_system     STRING  NOT NULL,
    entity            STRING  NOT NULL,
    watermark_type    STRING  NOT NULL,
    watermark_value   STRING,
    run_id            STRING  NOT NULL,
    updated_at        TIMESTAMP NOT NULL
) USING DELTA
COMMENT 'Watermark theo bước/entity (timestamp hoặc delta_version, lưu dạng chuỗi)';

-- State: sổ cái file đã ingest (append-only sự kiện; trạng thái hiện tại = sự kiện mới nhất theo file_id)
CREATE TABLE IF NOT EXISTS meta.state_file_manifest (
    file_id           STRING  NOT NULL,
    source_system     STRING  NOT NULL,
    entity            STRING  NOT NULL,
    file_path         STRING  NOT NULL,
    file_name         STRING  NOT NULL,
    file_size         BIGINT,
    file_modified_at  TIMESTAMP,
    checksum          STRING,
    batch_id          STRING,
    status            STRING  NOT NULL,
    run_id            STRING,
    task_id           STRING,
    event_at          TIMESTAMP NOT NULL
) USING DELTA
COMMENT 'File manifest: NEW / LOADED / DUPLICATE / FAILED';

-- Log: sự kiện pipeline (append-only)
CREATE TABLE IF NOT EXISTS meta.log_pipeline_run (
    run_id            STRING  NOT NULL,
    pipeline          STRING  NOT NULL,
    load_date         DATE    NOT NULL,
    status            STRING  NOT NULL,
    message           STRING,
    event_at          TIMESTAMP NOT NULL
) USING DELTA
COMMENT 'Sự kiện chạy pipeline';

-- Log: sự kiện task (append-only: RUNNING lúc bắt đầu, SUCCESS/FAILED/SKIPPED lúc kết thúc)
CREATE TABLE IF NOT EXISTS meta.log_task_run (
    task_id           STRING  NOT NULL,
    run_id            STRING  NOT NULL,
    load_date         DATE    NOT NULL,
    layer             STRING  NOT NULL,
    step              STRING  NOT NULL,
    source_system     STRING,
    entity            STRING,
    target_table      STRING,
    status            STRING  NOT NULL,
    rows_read         BIGINT,
    rows_inserted     BIGINT,
    rows_updated      BIGINT,
    rows_deleted      BIGINT,
    rows_rejected     BIGINT,
    rows_deduped      BIGINT,
    watermark_from    STRING,
    watermark_to      STRING,
    delta_version     BIGINT,
    error_message     STRING,
    started_at        TIMESTAMP NOT NULL,
    ended_at          TIMESTAMP,
    event_at          TIMESTAMP NOT NULL
) USING DELTA
COMMENT 'Sự kiện task theo step/entity';

-- Schema evolution có kiểm soát (P11): cột mới phát hiện ở Bronze → DETECTED → APPROVED → ACTIVE
CREATE TABLE IF NOT EXISTS meta.schema_registry (
    table_name        STRING  NOT NULL,
    column_name       STRING  NOT NULL,
    data_type         STRING  NOT NULL,
    layer             STRING  NOT NULL,
    status            STRING  NOT NULL,
    first_seen_batch  STRING,
    detected_at       TIMESTAMP NOT NULL,
    approved_at       TIMESTAMP
) USING DELTA
COMMENT 'Đăng ký cột theo bảng';

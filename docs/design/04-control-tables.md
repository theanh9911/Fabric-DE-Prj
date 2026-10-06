# 04 — Bảng điều khiển (`meta`)

> Mỗi bảng một mục đích. Không nhét run log, watermark, DQ, recon chung một bảng. Tạo bảng **đúng bước cần**.

## 1. Danh sách

| Bảng | 1 dòng = | Dùng để | Ai ghi | Tạo ở |
|---|---|---|---|---|
| `cfg_source_entity` | 1 entity nguồn | Pipeline biết lấy gì, chiến lược nào | `nb_setup_config` (Git) | Bước 1.2 (sửa cột) |
| `watermark_state` | trạng thái **hiện hành** của 1 entity | Lần sau đọc tiếp từ đâu | `nb_brz_load` | Bước 1.2 |
| `ingestion_batch` | **1 lần thử** ingest 1 entity (db) / 1 file (file) / 1 entity không có gì mới | Đã lấy gì, cửa sổ nào, bao nhiêu, trạng thái | `nb_brz_load` | Bước 1.2 |
| `task_run` | 1 bước trong lượt chạy, mỗi lần thử 1 dòng | Bước nào lỗi, bao lâu, bao nhiêu dòng | `nb_brz_load`, sau là runner | Bước 1.2 |
| `pipeline_run` | 1 lượt chạy pipeline tổng | Trạng thái, thời gian, tham số, lỗi | `nb_ops_run_end` | Bước 1.2 |
| `dq_result` | 1 lần đánh giá 1 rule | Rule, số lỗi, pass/fail | `nb_dq` | Bước 3 |
| `reconciliation_result` | 1 phép đối soát | Hai phía, chênh lệch, ngưỡng | `nb_ops_recon` | Bước 3 (cơ bản), 6 (đủ) |

Bỏ so với v2: `cfg_pipeline_step`, `cfg_dq_rule`, `ref_*` (→ code), `state_file_manifest` (→ `ingestion_batch`), `schema_registry` (→ `task_run.details_json`; thêm bảng nếu drill Q16 cần).

## 2. Định danh — đừng nhầm

| Định danh | Nghĩa | Ổn định khi rerun? | Ví dụ |
|---|---|---|---|
| `run_id` | 1 lượt chạy pipeline | không — mỗi lượt mới | RunId của Fabric |
| `ingestion_batch_id` | **1 dòng** của `ingestion_batch` = 1 lần thử | không — mỗi lần thử mới | `uuid` |
| `batch_id` | **Dữ liệu** của 1 entity trong 1 lượt normal = thứ được replay | **có** — rerun giữ nguyên | `retail_orders_20260102_ab12cd34` |
| `landing_path` | Thư mục landing chứa dữ liệu của `batch_id` | **có** | `Files/landing/retail/orders/load_date=2026-01-02/batch=<RunId>/` |
| `load_date` | Ngày logic; backfill có thể chạy nhiều `load_date` | có | `2026-01-02` |

- `batch_id` sinh **1 lần** ở lượt normal: `<source>_<entity>_<yyyymmdd>_<8 ký tự đầu RunId>`.
- Bronze `_batch_id` = `batch_id` → rerun `DELETE WHERE _batch_id = X` rồi ghi lại đúng phần đó.
- Khoá `ingestion_batch`: `ingestion_batch_id`. Tìm input để replay: `batch_id` + `status = COMMITTED` + `run_mode IN (normal, reprocess)`.

## 3. Cột

### `cfg_source_entity`
```
source_system, entity                 khoá
load_strategy      db_incremental | db_full_snapshot | file_new_or_changed
source_object      dbo.orders | inbound/retail/orders
watermark_column   updated_at (db_incremental)
lookback_minutes   1440 (db_incremental, file)
settle_minutes     5 (file)
source_timezone    múi giờ của timestamp nguồn (Silver đổi sang UTC) — ERP: chốt ở EDA
business_keys      order_no,product_code
load_order, is_active
```

### `watermark_state` — 1 dòng / entity, MERGE
```
source_system, entity                 khoá
watermark_value          STRING 'yyyy-MM-dd HH:mm:ss' — miền thời gian theo chiến lược (ERP: đồng hồ nguồn; file: UTC thật)
last_successful_batch_id
updated_at_utc
```

### `ingestion_batch` — append-only, 1 dòng / lần thử
```
ingestion_batch_id       khoá, uuid
batch_id                 định danh dữ liệu (ổn định khi rerun)
run_id, load_date
run_mode                 normal | rerun | reprocess
attempt_number           1, 2… (retry trong cùng run)
source_system, entity, load_strategy
window_start, window_end
watermark_before, watermark_after
landing_path
source_file_name         file: tên file nguồn (1 dòng / file); db: NULL
landing_file_size
content_hash             file: sha2 nội dung → nhận file trùng
copy_rows_read           db: rowsRead của Copy (NULL với file Binary)
copy_files_written       file: filesWritten của Copy cho entity
bronze_row_count
status                   COMMITTED | SKIPPED_DUPLICATE | NO_NEW_DATA | FAILED
started_at_utc, committed_at_utc
error_message
```
- **NO_NEW_DATA:** entity không có file/dòng mới vẫn có 1 dòng → DQ freshness biết "đã kiểm, không có gì", khác với "chưa chạy".
- **SKIPPED_DUPLICATE:** file có `content_hash` trùng file đã COMMITTED → không vào Bronze.

### `pipeline_run` — append-only, ghi khi kết thúc
```
run_id, pipeline_name, environment (dev|test|prod), trigger_type (manual|schedule|backfill)
load_date, run_mode
status                   SUCCEEDED | FAILED
started_at_utc           pipeline().TriggerTime
ended_at_utc
parameters_json, error_message, code_version
```

### `task_run` — append-only, mỗi lần thử 1 dòng
```
task_run_id, run_id, task_name, layer, entity, attempt_number
status                   SUCCEEDED | FAILED
started_at_utc, ended_at_utc
input_rows, output_rows, quarantined_rows
delta_version            version bảng đích sau bước (RESTORE)
error_message, details_json   (vd cột mới — schema drift)
```

### `dq_result` — append-only
```
run_id, task_run_id, entity, rule_id, severity (critical|warning),
rows_checked, rows_failed, threshold, status (PASS|WARN|FAIL), evaluated_at_utc
```

### `reconciliation_result` — append-only
```
run_id, load_date, check_name, grain (batch|day|month), unit (rows|files|amount),
source_value, target_value, difference, tolerance, status, checked_at_utc
```

## 4. Luồng ghi

```
pl_ingest (run_id = R)
 ├ nb_brz_load
 │   ├ mỗi entity / file: ingestion_batch (COMMITTED | SKIPPED_DUPLICATE | NO_NEW_DATA | FAILED)
 │   ├ task_run (brz, entity, rows, delta_version)
 │   └ watermark_state MERGE — chỉ entity COMMITTED / NO_NEW_DATA, sau khi mọi entity xong
 └ nb_ops_run_end (nhánh success và failure)
     └ pipeline_run → FAILED: ghi xong rồi raise để pipeline vẫn báo lỗi
```

- Không có dòng RUNNING: trạng thái đang chạy xem ở Monitoring hub (đỡ 1 phiên Spark mỗi lượt — capacity trial).
- Retry tạo dòng mới (`attempt_number + 1`), không ghi đè.
- Không log từng dòng dữ liệu; dòng lỗi cụ thể ở `quarantine_*` kèm `run_id` / `batch_id`.

## Quyết định
- 7 bảng; `ingestion_batch_id` cho lần thử, `batch_id` + `landing_path` cho dữ liệu replay; trạng thái `NO_NEW_DATA` cho entity không có gì mới.

## Còn mở
- Nếu SQL endpoint trễ gây phiền khi chạy liên tiếp: chuyển `cfg_*`, `watermark_state`, `ingestion_batch` sang Fabric SQL Database (pipeline ghi bằng Stored procedure). Chỉ làm khi đo thấy vấn đề.

# 04 — Bảng điều khiển (`meta`)

> Mỗi bảng một mục đích. Không nhét run log, watermark, DQ, recon chung một bảng. Tạo bảng **đúng bước cần**.

## 1. Danh sách

| Bảng | 1 dòng = | Dùng để | Ai ghi | Tạo ở |
|---|---|---|---|---|
| `cfg_source_entity` | 1 entity nguồn | Pipeline biết lấy gì, chiến lược nào | `nb_setup_config` (Git) | ✅ có (sửa cột ở Bước 1) |
| `watermark_state` | trạng thái **hiện hành** của 1 entity | Lần sau đọc tiếp từ đâu | `nb_brz_load` | Bước 1 |
| `ingestion_batch` | 1 lần ingest 1 entity (db) / 1 file (file) | Đã lấy gì, cửa sổ nào, bao nhiêu dòng | `nb_brz_load` | Bước 1 |
| `pipeline_run` | 1 lượt chạy pipeline tổng | Trạng thái, thời gian, tham số, lỗi | notebook cuối pipeline | Bước 1 (cho `pl_ingest`) |
| `task_run` | 1 bước (notebook/activity) trong lượt chạy, mỗi lần thử 1 dòng | Bước nào lỗi, chạy bao lâu, bao nhiêu dòng | runner / notebook | Bước 1 |
| `dq_result` | 1 lần đánh giá 1 rule | Rule, số lỗi, pass/fail | `nb_dq` | Bước 3 |
| `reconciliation_result` | 1 phép đối soát | Hai phía, chênh lệch, ngưỡng | `nb_ops_recon` | Bước 3 (cơ bản), 6 (đủ) |

**Bỏ** so với v2: `cfg_pipeline_step`, `cfg_dq_rule`, `ref_*` (→ code), `state_file_manifest` (→ thay bằng `ingestion_batch`), `schema_registry` (→ drift ghi trong `task_run` + bảng riêng nếu drill Q16 cần).

## 2. Cột

### `cfg_source_entity`
```
source_system, entity                 khoá
load_strategy      db_incremental | db_full_snapshot | file_new_or_changed
source_object      dbo.orders | inbound/retail/orders
watermark_column   updated_at (db_incremental)
lookback_minutes   1440 (db_incremental)
settle_minutes     5 (file)
source_timezone    UTC | Asia/Ho_Chi_Minh
business_keys      order_no,product_code
load_order, is_active
```

### `watermark_state` — 1 dòng / entity, MERGE
```
source_system, entity                 khoá
watermark_value          STRING (timestamp ISO)
last_successful_batch_id
updated_at_utc
```
Lịch sử trước/sau nằm ở `ingestion_batch` (không cần giữ ở đây).

### `ingestion_batch` — append-only
```
batch_id                 <source>_<entity>_<yyyymmdd>_<run_id_ngắn>
run_id, load_date, run_mode (normal|rerun|reprocess)
source_system, entity, load_strategy
window_start, window_end                  (db_incremental, file)
watermark_before, watermark_after
source_file_path, source_file_size, source_file_modified_at   (file: 1 dòng / file)
landing_path
source_row_count                          (Copy rowsRead nếu có)
bronze_row_count
status                   COMMITTED | FAILED | SKIPPED_DUPLICATE
started_at_utc, committed_at_utc
error_message
```

### `pipeline_run` — append-only
```
run_id                   pipeline().RunId
pipeline_name, environment (dev|test|prod), trigger_type (manual|schedule|backfill)
load_date
status                   SUCCEEDED | FAILED
started_at_utc           pipeline().TriggerTime
ended_at_utc
parameters_json
error_message
code_version             commit/tag nếu lấy được
```

### `task_run` — append-only, mỗi lần thử 1 dòng
```
task_run_id, run_id, task_name, layer, entity
attempt_number
status                   SUCCEEDED | FAILED
started_at_utc, ended_at_utc
input_rows, output_rows, quarantined_rows
delta_version            version bảng đích sau bước (RESTORE)
error_message, details_json   (vd cột mới phát hiện — schema drift)
```

### `dq_result` — append-only
```
run_id, task_run_id, entity, rule_id, severity (critical|warning),
rows_checked, rows_failed, threshold, status (PASS|WARN|FAIL), evaluated_at_utc
```

### `reconciliation_result` — append-only
```
run_id, load_date, check_name, grain (day|month|batch),
source_value, target_value, difference, tolerance, status, checked_at_utc
```

## 3. Luồng ghi

```
pl_ingest (RunId = R)
 ├ nb_brz_load
 │   ├ mỗi entity: ingestion_batch (COMMITTED|FAILED|SKIPPED_DUPLICATE)
 │   ├ task_run (brz, entity, rows, delta_version)
 │   └ watermark_state MERGE  ← chỉ entity COMMITTED
 └ nb_ops_run_end (luôn chạy: nhánh success và failure)
     └ pipeline_run (SUCCEEDED|FAILED, error)  → nếu FAILED: notebook tự fail để pipeline vẫn báo lỗi
```

- **Không có dòng RUNNING trong bảng:** trạng thái đang chạy xem ở Monitoring hub. Ghi 1 dòng khi kết thúc → đỡ 1 phiên Spark mỗi lượt (capacity trial). Nếu sau này cần RUNNING (dashboard realtime) → thêm notebook đầu pipeline.
- **Bảng log không được làm pipeline trông thành công:** lỗi → ghi FAILED **rồi** fail tiếp.
- **Retry** tạo `task_run` mới (`attempt_number + 1`), không ghi đè.
- Không log từng dòng dữ liệu; dòng lỗi cụ thể nằm ở `quarantine_*` kèm `run_id`/`batch_id`.

## 4. Ba định danh — đừng nhầm
| | Nghĩa | Ví dụ |
|---|---|---|
| `run_id` | 1 lượt chạy pipeline | RunId của Fabric |
| `batch_id` | 1 entity (hoặc 1 file) trong 1 lượt | `retail_orders_20260102_ab12` |
| `load_date` | ngày logic; backfill có thể chạy nhiều `load_date` | `2026-01-02` |

## Quyết định
- 7 bảng như §1; `pipeline_run` ghi 1 dòng khi kết thúc.
- Tất cả ở `lh_platform` schema `meta`, ghi bằng Spark; pipeline đọc qua SQL endpoint.

## Còn mở
- Phương án thay thế nếu SQL endpoint trễ gây phiền (backfill liên tục): chuyển `cfg_*`, `watermark_state`, `ingestion_batch` sang **Fabric SQL Database** (pipeline ghi bằng Stored procedure, không trễ). Chỉ làm khi đo thấy vấn đề.

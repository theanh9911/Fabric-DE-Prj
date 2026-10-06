# 04 — Log & bảng điều khiển (`meta`)

> **Fabric giữ log kỹ thuật; `meta` chỉ giữ thứ Fabric không biết** — dữ liệu nào đã được chấp nhận, đã đọc tới đâu, chất lượng ra sao, có khớp không. Không nhân bản Monitoring hub.

## 1. Ai giữ log gì

| Thông tin | Nơi giữ | Ghi chú |
|---|---|---|
| Pipeline / activity: bắt đầu, kết thúc, trạng thái, thời lượng, lỗi, retry | **Fabric Monitoring hub** | Tra cứu khi debug; pipeline lỗi thì trạng thái Fabric là FAILED — không cần notebook ghi lại |
| Copy output: `rowsRead`, `filesWritten`, dung lượng, throughput | **Fabric** (output activity) | Lưu vào `meta` chỉ khi đối soát **tự động** cần (Bước 6) |
| Spark / notebook run, chẩn đoán | **Fabric** | |
| Version bảng sau mỗi lần ghi | **Delta history** (`DESCRIBE HISTORY`) | Mỗi lần ghi gắn `run_id` vào `userMetadata` → tìm version theo run để RESTORE |
| Dữ liệu nào đã vào Bronze: cửa sổ, file, hash, `batch_id`, trạng thái | **`meta.ingestion_batch`** | Fabric không biết khái niệm này |
| Mốc đã đọc tới đâu | **`meta.watermark_state`** | |
| Rule DQ đã chạy, số lỗi, hành động | **`meta.dq_result`** (Bước 3) | |
| Đối soát Source → Bronze → Silver → Gold | **`meta.reconciliation_result`** (Bước 3/6) | |
| Trạng thái end-to-end theo `run_id` cho report | `meta.run_summary` — **chỉ thêm nếu** `rpt_pipeline_health` cần (Bước 8) | |

Lý do giữ dài hạn trong `meta`: Monitoring hub có giới hạn thời gian lưu và không query được bằng SQL cùng dữ liệu; nhưng chỉ giữ phần **audit dữ liệu**, không giữ phần **vận hành**.

## 2. Bảng

| Bảng | 1 dòng = | Ai ghi | Tạo ở |
|---|---|---|---|
| `cfg_source_entity` | 1 entity nguồn | `nb_setup_config` (Git) | Bước 1.2 (sửa cột) |
| `watermark_state` | trạng thái hiện hành của 1 entity | `nb_brz_load` | Bước 1.2 |
| `ingestion_batch` | 1 lần thử ingest 1 entity (db) / 1 file / 1 entity không có gì mới | `nb_brz_load` | Bước 1.2 |
| `dq_result` | 1 lần đánh giá 1 rule | `nb_dq` | Bước 3 |
| `reconciliation_result` | 1 phép đối soát | `nb_ops_recon` | Bước 3 / 6 |
| `run_summary` *(tuỳ chọn)* | 1 lượt chạy end-to-end | notebook cuối `pl_master_daily` | Bước 8 nếu cần |

Bỏ so với các bản trước: `cfg_pipeline_step`, `cfg_dq_rule`, `ref_*` (→ code) · `state_file_manifest` (→ `ingestion_batch`) · `schema_registry` (→ `ingestion_batch.schema_changes`) · `pipeline_run`, `task_run`, `log_*` (→ Monitoring hub + Delta history).

## 3. Định danh — đừng nhầm

| Định danh | Nghĩa | Rerun giữ nguyên? | Ví dụ |
|---|---|---|---|
| `run_id` | 1 lượt chạy pipeline (khớp Monitoring hub) | không | RunId của Fabric |
| `ingestion_batch_id` | 1 dòng `ingestion_batch` = 1 lần thử | không | uuid |
| `batch_id` | **Dữ liệu** của 1 entity trong 1 lượt normal — thứ được replay | **có** | `retail_orders_20260102_ab12cd34` |
| `landing_path` | Thư mục landing của `batch_id` | **có** | `Files/landing/retail/orders/load_date=2026-01-02/batch=<RunId>/` |
| `load_date` | Ngày logic | có | `2026-01-02` |

- `batch_id` sinh 1 lần ở lượt normal: `<source>_<entity>_<yyyymmdd>_<8 ký tự đầu RunId>`; Bronze `_batch_id` = giá trị này.
- Tìm input để replay: `batch_id`, `status = COMMITTED`, `run_mode IN (normal, reprocess)`.

## 4. Cột

### `cfg_source_entity`
```
source_system, entity                 khoá
load_strategy      db_incremental | db_full_snapshot | file_new_or_changed
source_object      dbo.orders | inbound/retail/orders
watermark_column   updated_at (db_incremental)
lookback_minutes   1440 (db_incremental, file)
settle_minutes     5 (file)
source_timezone    múi giờ timestamp nguồn (Silver đổi sang UTC) — ERP: chốt ở EDA
business_keys      order_no,product_code
load_order, is_active
```

### `watermark_state` — 1 dòng / entity, MERGE
```
source_system, entity                 khoá
watermark_value          STRING 'yyyy-MM-dd HH:mm:ss' — đồng hồ theo chiến lược (ERP: nguồn; file: UTC thật)
last_successful_batch_id
updated_at_utc
```
**Không bao giờ lùi:** `watermark mới = max(watermark hiện tại, giá trị của batch)` (design/03 §3).

### `ingestion_batch` — append-only, 1 dòng / lần thử
```
ingestion_batch_id       khoá, uuid
batch_id                 định danh dữ liệu (ổn định khi rerun)
run_id, load_date
run_mode                 normal | rerun | reprocess
source_system, entity, load_strategy
window_start, window_end
watermark_before, watermark_after
landing_path
source_file_name         file: 1 dòng / file; db: NULL
landing_file_size
content_hash             file: sha2 nội dung → nhận file trùng
bronze_row_count
schema_changes           cột mới / mất so với Bronze (schema drift), NULL nếu không
status                   COMMITTED | SKIPPED_DUPLICATE | NO_NEW_DATA | FAILED
started_at_utc, committed_at_utc
error_message
```
- `NO_NEW_DATA`: entity không có gì mới vẫn có 1 dòng → freshness biết "đã kiểm, không có gì" khác "chưa chạy".
- `SKIPPED_DUPLICATE`: file có `content_hash` trùng file đã COMMITTED → không vào Bronze.
- Cột Copy output (`copy_rows_read`, `copy_files_written`) **chưa thêm**; thêm ở Bước 6 nếu đối soát tự động cần (spike K7).

### `dq_result` — append-only (Bước 3)
```
run_id, entity, rule_id, severity (critical|warning),
rows_checked, rows_failed, threshold, status (PASS|WARN|FAIL), evaluated_at_utc
```

### `reconciliation_result` — append-only (Bước 3/6)
```
run_id, load_date, check_name, grain (batch|day|month), unit (rows|files|amount),
source_value, target_value, difference, tolerance, status, checked_at_utc
```

## 5. Luồng ghi (Bước 1)

```
pl_ingest (run_id = R)                                   Monitoring hub: trạng thái pipeline/activity
 └ nb_brz_load
     ├ mỗi entity / file → ingestion_batch (COMMITTED | SKIPPED_DUPLICATE | NO_NEW_DATA | FAILED)
     ├ ghi Bronze với userMetadata = R                    Delta history: version theo run
     ├ mọi entity xong → watermark_state MERGE (không lùi)
     └ lỗi → ghi FAILED rồi raise → activity & pipeline FAILED trên Fabric
```

## Quyết định
- Log vận hành ở Fabric; `meta` chỉ audit dữ liệu (ingestion, watermark, DQ, recon). Bước 1 có 3 bảng.
- RESTORE dựa vào Delta history + `userMetadata = run_id`.

## Còn mở
- `run_summary`: chỉ khi `rpt_pipeline_health` cần (Bước 8).
- Nếu SQL endpoint trễ gây phiền khi chạy liên tiếp: chuyển `cfg_source_entity`, `watermark_state`, `ingestion_batch` sang Fabric SQL Database. Chỉ làm khi đo thấy vấn đề.

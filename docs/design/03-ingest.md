# 03 — Ingest (Source → landing)

> **Generic ở điều phối và logging; chiến lược lấy dữ liệu theo từng loại nguồn.** Không ép ERP và file vào cùng một cơ chế watermark chỉ để code trông chung.

## 1. Ba chiến lược

| Chiến lược | Áp dụng | Lấy gì | Biết "mới" bằng |
|---|---|---|---|
| `db_incremental` | ERP orders | `WHERE updated_at > start AND updated_at <= end` | cột `updated_at` (cửa sổ nguồn) |
| `db_full_snapshot` | ERP customers / products / sales_hierarchy | `SELECT *` toàn bảng | không cần — mỗi lần là ảnh đầy đủ |
| `file_new_or_changed` | retail *, reference categories | file có `start ≤ LastModified < end` | thời điểm sửa file + metadata file |

Cột `cfg_source_entity.load_strategy` quyết định nhánh (thay cho cặp `source_type` + `load_type`).

## 2. Cửa sổ nguồn (source window)

Mỗi lượt ingest 1 entity đọc đúng 1 cửa sổ `(window_start, window_end]` và **ghi lại cửa sổ đó** vào `meta.ingestion_batch`.

| | `window_start` | `window_end` |
|---|---|---|
| db_incremental | `watermark_state.watermark_value − lookback` | `run_start` (UTC, giờ thật) |
| file_new_or_changed | `watermark_state.watermark_value` | `run_start − settle_minutes` |
| db_full_snapshot | — | `run_start` (thời điểm chụp) |

- `load_date` (ngày giả lập) **không thay thế** watermark — nó chỉ là nhãn logic của lượt chạy (thư mục landing, `_load_date`).
- **Watermark mới** (chỉ ghi khi batch đã COMMITTED ở Bronze):
  - db_incremental: `max(updated_at)` của dữ liệu đã lấy (0 dòng → giữ nguyên). Lý do: `updated_at` là giờ giả lập; lấy giờ chạy sẽ bỏ sót ngày sau.
  - file: `window_end`.
- **Biên an toàn file (`settle_minutes`, mặc định 5):** bỏ qua file vừa sửa gần `run_start` vì có thể đang ghi dở; lượt sau sẽ lấy.
- **Dòng tương lai** (ERP `updated_at` 2027 > `run_start`): nằm ngoài cửa sổ → không vào, không đẩy watermark → lộ ra ở recon Source↔Bronze.

## 3. Ba kiểu chạy — phải phân biệt

| Kiểu (`p_mode`) | Khi nào | Lấy dữ liệu | Kết quả |
|---|---|---|---|
| **normal** | Lịch hằng ngày; hoặc chạy lại sau khi lượt trước **lỗi trước khi COMMITTED** (watermark chưa tiến → tự lấy lại đúng phần đó) | Copy từ nguồn, cửa sổ từ `watermark_state` | Lấy phần mới |
| **rerun** (cùng input) | Chạy lại 1 `load_date` đã COMMITTED (vd sửa code Bronze, kiểm idempotent) | **Không Copy lại.** Đọc lại đúng thư mục landing đã ghi trong `ingestion_batch` của `load_date` đó | Giống hệt lần trước; watermark **không đổi** |
| **reprocess** | Nguồn đã sửa dữ liệu cũ; cần lấy lại một khoảng | Copy từ nguồn với `p_window_start` chỉ định | Kết quả **có thể khác** — đúng, vì input khác; ghi `run_mode = reprocess` |

- "Chạy lại ra cùng kết quả" (S2) chỉ áp dụng cho **rerun**: cùng input (landing bất biến) + cùng phiên bản code.
- Rerun dùng landing chính là lý do landing phải **bất biến** (P5): replay không phụ thuộc nguồn còn giữ dữ liệu cũ hay không (ERP master đã bị ghi đè, file có thể bị xoá).

## 4. Pipeline `pl_ingest`

```
Parameters  p_load_date, p_mode (normal | rerun | reprocess; mặc định normal), p_window_start (chỉ reprocess)
set_run_start         v_run_start = utcNow('yyyy-MM-dd HH:mm:ss')
lkp_source_entity     T-SQL: cfg_source_entity ⋈ watermark_state
                      → mỗi entity: load_strategy, source_object, watermark_column, window_start, window_end, watermark_before
fe_source_entity      items = rerun ? [] : output Lookup          (rerun bỏ qua Copy)
  sw_source_type      on = startsWith(load_strategy, 'db_') ? 'db' : 'file'
    db      → cp_db_to_landing     db_full_snapshot: SELECT * ; db_incremental: WHERE theo cửa sổ
    file    → cp_file_to_landing   Binary, lọc LastModified [window_start, window_end)
    default → fail_unknown_source_type
nb_brz_load(p_load_date, p_run_id, p_mode, p_plan = output Lookup dạng JSON)
                      → Bronze + ingestion_batch + task_run + watermark_state
nb_ops_run_end(SUCCEEDED | FAILED)  → pipeline_run; FAILED thì notebook tự fail
```

Switch vẫn 2 nhánh (`db`, `file`); khác biệt full/incremental nằm ở biểu thức câu query → không phải nhân đôi activity Copy.

Landing: `Files/landing/<source>/<entity>/load_date=<d>/batch=<RunId>/` — mỗi lượt 1 thư mục mới, không ghi đè (raw bất biến, replay được).

## 5. File được gửi lại / trùng

| Tình huống | Nhận ra bằng | Xử lý |
|---|---|---|
| File mới | path chưa có trong `ingestion_batch` | nạp |
| Gửi lại cùng tên, nội dung đổi | path đã có, `size`/`modified_at` khác | nạp như **phiên bản mới**; Silver lấy bản `updated_at` mới nhất theo khoá |
| Gửi lại y hệt | path + size + modified giống | bỏ qua (ghi `SKIPPED_DUPLICATE`) |
| Bản copy khác tên | không nhận ra ở ingest | Silver dedup theo khoá; checksum (giai đoạn sau) |

## 6. Xoá ở nguồn
- Watermark **không** phát hiện hard delete.
- Master: so snapshot hôm nay với hôm trước ở Silver → khoá biến mất = xoá (đánh dấu `is_deleted`, không xoá vật lý).
- Orders: hợp đồng nói không xoá; nếu nguồn có **CDC** thì CDC là lựa chọn ưu tiên để nhận cả update lẫn delete (ghi nhận cho dự án thật).

## Quyết định
- Copy activity cho mọi nguồn (ADR 012). Ba chiến lược như §1. Cửa sổ ghi vào `ingestion_batch`; watermark hiện hành ở `watermark_state`.

## Còn mở (spike)
- **K5:** Lấy `size` / `LastModified` của **file nguồn** để ghi `ingestion_batch`: Copy *session log* (danh sách file đã chép) hay activity Get Metadata — thử ở Bước 1.
- **K6:** Notebook chạy từ pipeline với `%%configure -f` (standard session) — kiểm khi nối `nb_brz_load`.

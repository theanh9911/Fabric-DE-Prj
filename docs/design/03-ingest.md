# 03 — Ingest (Source → landing)

> **Generic ở điều phối và logging; chiến lược lấy dữ liệu theo từng loại nguồn.** Không ép ERP và file vào cùng một cơ chế watermark chỉ để code trông chung.

## 1. Ba chiến lược

| Chiến lược | Áp dụng | Lấy gì | Biết "mới" bằng |
|---|---|---|---|
| `db_incremental` | ERP orders | `WHERE updated_at > window_start AND updated_at < window_end` | cột `updated_at` — **đồng hồ của nguồn** |
| `db_full_snapshot` | ERP customers / products / sales_hierarchy | `SELECT *` toàn bảng | không cần — mỗi lần là ảnh đầy đủ |
| `file_new_or_changed` | retail *, reference categories | file có `window_start ≤ LastModified < window_end`, rồi **bỏ file trùng nội dung** | LastModified (giờ thật của storage) + hash nội dung |

Cột `cfg_source_entity.load_strategy` quyết định nhánh.

## 2. Miền thời gian — quy tắc quan trọng nhất

**Mỗi cửa sổ chỉ so sánh các giá trị trong CÙNG một đồng hồ.**

| Chiến lược | So sánh với | Đồng hồ | Cận trên `window_end` |
|---|---|---|---|
| db_incremental | `updated_at` (giá trị trong bảng ERP) | **đồng hồ nguồn** | "bây giờ" của nguồn. Ở dự án này = **cuối ngày `p_load_date`** (virtual clock): `p_load_date + 1 ngày`, loại trừ |
| file_new_or_changed | `LastModified` (do storage gán khi ghi file) | **giờ thật UTC** | `run_start − settle_minutes` |
| db_full_snapshot | — | — | — (ghi `snapshot_at = run_start` để truy vết) |

- Vì sao không dùng `run_start` cho ERP: `updated_at` là giờ giả lập (2026-01-…), `run_start` là giờ thật (2026-10-…). So hai đồng hồ khác nhau thì điều kiện vô nghĩa (luôn đúng hôm nay, sai khi đồng hồ giả lập vượt giờ thật).
- **Map sang dự án thật:** cận trên ERP = giờ hiện tại **của chính nguồn** (vd `SELECT SYSUTCDATETIME()` trên nguồn, hoặc `run_start` nếu đã xác nhận hai đồng hồ cùng chuẩn UTC và lệch không đáng kể).
- **Múi giờ** của `updated_at` (UTC hay giờ VN) **không ảnh hưởng cửa sổ** (so cùng miền); chỉ ảnh hưởng việc đổi sang UTC ở Silver → `cfg_source_entity.source_timezone`, chốt ở EDA.
- Dòng `updated_at` năm 2027 > cuối ngày `p_load_date` → nằm ngoài cửa sổ → không vào Bronze, không đẩy watermark → lộ ở recon Source ↔ Bronze (đếm riêng "dòng tương lai").

## 3. Cửa sổ & watermark

| | `window_start` | `window_end` | Watermark mới (khi COMMITTED) |
|---|---|---|---|
| db_incremental | `watermark − lookback` (chưa có → 1900-01-01) | `p_load_date + 1 ngày` | `max(updated_at)` đã lấy (0 dòng → giữ nguyên) |
| file_new_or_changed | `watermark − lookback` | `run_start − settle` | `window_end` |
| db_full_snapshot | — | — | — |

- `load_date` là **nhãn logic** của lượt chạy (thư mục landing, `_load_date`) **và** là đồng hồ nguồn cho ERP trong mô phỏng. Nó không thay watermark.
- Lookback (mặc định 1440 phút) cho cả ERP và file: bắt dòng commit trễ / file ghi đè có thời gian lệch. Phần lấy dư được loại ở bước dedup (ERP: Silver theo khoá; file: hash nội dung ở Bronze).
- Settle (5 phút): bỏ qua file vừa sửa, có thể đang ghi dở.
- Cửa sổ thực tế của mỗi lượt ghi vào `meta.ingestion_batch`.

## 4. Ba kiểu chạy

| Kiểu (`p_mode`) | Khi nào | Lấy dữ liệu | Kết quả |
|---|---|---|---|
| **normal** | Lịch hằng ngày; hoặc sau lượt lỗi **trước khi COMMITTED** (watermark chưa tiến → tự lấy lại) | Copy từ nguồn, cửa sổ từ `watermark_state` | Lấy phần mới |
| **rerun** (cùng input) | Chạy lại 1 `load_date` đã COMMITTED (sửa code Bronze, kiểm idempotent) | **Không Copy.** Đọc lại đúng `landing_path` của các batch COMMITTED của `load_date` đó | Bronze y hệt; watermark **không đổi** |
| **reprocess** | Nguồn đã sửa dữ liệu cũ; cần lấy lại một khoảng | Copy với `p_window_start` chỉ định (Bước 7) | Có thể khác — đúng, vì input khác |

- S2 ("chạy lại ra cùng kết quả") chỉ áp dụng cho **rerun** (cùng input = landing bất biến + cùng code).
- Rerun dùng landing vì nguồn có thể không còn giữ dữ liệu cũ (ERP master đã ghi đè, file có thể bị xoá).

## 5. File: gửi lại, trùng, đến muộn

Hợp đồng nguồn (design/01): file gửi lại = **ghi đè cùng tên**; storage gán LastModified = thời điểm ghi; nguồn **không** giữ thời gian sửa cũ.

| Tình huống | Ingest nhìn thấy? | Xử lý |
|---|---|---|
| File mới | có (LastModified trong cửa sổ) | nạp |
| Ghi đè cùng tên, nội dung đổi | có (LastModified mới) | hash khác → nạp như **phiên bản mới**; Silver lấy bản `updated_at` mới nhất theo khoá |
| Ghi đè y hệt / bị chép lại lần nữa do lookback | có | hash trùng file đã COMMITTED → `SKIPPED_DUPLICATE`, không vào Bronze |
| Bản copy khác tên | có | hash trùng → `SKIPPED_DUPLICATE` |
| File đến với LastModified cũ (vd nguồn giữ thời gian gốc khi upload) | chỉ khi còn trong lookback | ngoài lookback → **không thấy**; ghi rõ là vi phạm hợp đồng; phát hiện bằng DQ freshness / recon số file |

Hash = `sha2(nội dung file)` tính trong `nb_brz_load` (đọc landing bằng `binaryFile`) — rẻ với file nhỏ, không phụ thuộc metadata nguồn (spike K5).

## 6. Xoá ở nguồn
- Watermark không phát hiện hard delete.
- Master: so snapshot liên tiếp ở Silver → khoá biến mất = xoá (`is_deleted`).
- Orders: hợp đồng nói không xoá. Nếu nguồn có **CDC**, CDC là lựa chọn ưu tiên để nhận cả update lẫn delete.

## 7. Pipeline `pl_ingest`

```
Parameters  p_load_date, p_mode (normal | rerun | reprocess; mặc định normal), p_window_start (reprocess, Bước 7)
set_run_start         v_run_start = utcNow('yyyy-MM-dd HH:mm:ss')
lkp_source_entity     T-SQL: cfg_source_entity ⋈ watermark_state → mỗi entity:
                      load_strategy, source_object, watermark_column, watermark_before, window_start, window_end
fe_source_entity      items = rerun ? [] : output Lookup
  sw_source_type      on = startsWith(load_strategy, 'db_') ? 'db' : 'file'
    db      → cp_db_to_landing     full: SELECT * ; incremental: WHERE theo cửa sổ
    file    → cp_file_to_landing   Binary, lọc LastModified [window_start, window_end)
    default → fail_unknown_source_type
nb_brz_load(p_load_date, p_run_id, p_mode, p_plan = output Lookup JSON)
                      → Bronze + ingestion_batch + task_run + watermark_state
run_end_ok / run_end_fail → nb_ops_run_end → pipeline_run (FAILED thì notebook tự fail)
```

Landing: `Files/landing/<source>/<entity>/load_date=<d>/batch=<RunId>/` — thư mục mới mỗi lượt, không ghi đè.

## 8. Số liệu Copy dùng cho đối soát

| Nhánh | Copy output có | Dùng |
|---|---|---|
| db (table → parquet) | `rowsRead`, `rowsCopied` | so với số dòng Bronze của batch |
| file (Binary) | `filesRead`, `filesWritten`, `dataRead` — **không có số dòng** | so số file Copy ghi với số file trong `ingestion_batch`; số dòng chỉ biết sau parse (`bronze_row_count`) |

`nb_brz_load` nhận các số này qua tham số (output Copy) — chi tiết ở design/04.

## Quyết định
- ADR 012: Copy cho mọi nguồn; 3 chiến lược; cửa sổ ERP theo **đồng hồ nguồn** (virtual clock), file theo giờ thật + lookback + hash.

## Còn mở (spike)
- **K5:** metadata file **nguồn** (size, LastModified) cho `ingestion_batch` — Copy session log / Get Metadata. Không chặn: dedup đã dựa vào hash.
- **K6:** `%%configure -f` khi notebook chạy từ pipeline.
- **K7:** lấy output Copy (`rowsRead`, `filesWritten`) của từng entity trong ForEach để truyền cho `nb_brz_load` (Append variable trong ForEach, hoặc notebook đọc Copy log).

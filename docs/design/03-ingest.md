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
| db_incremental | `watermark − lookback` (chưa có → 1900-01-01) | `p_load_date + 1 ngày` | `max(watermark hiện tại, max(updated_at) đã lấy)` |
| file_new_or_changed | `watermark − lookback` | `run_start − settle` | `max(watermark hiện tại, window_end)` |
| db_full_snapshot | — | — | — |

- **Watermark không bao giờ lùi.** Vì có lookback, batch có thể chỉ chứa dòng *cũ hơn* watermark hiện tại (vd hôm nay không có dòng mới, chỉ đọc lại phần lookback) → `max(updated_at)` của batch < watermark. Lấy thẳng giá trị đó sẽ kéo watermark lùi và lần sau đọc lại nhiều hơn mỗi ngày. 0 dòng → giữ nguyên.

- `load_date` là **nhãn logic** của lượt chạy (thư mục landing, `_load_date`) **và** là đồng hồ nguồn cho ERP trong mô phỏng. Nó không thay watermark.
- Lookback (mặc định 1440 phút) cho cả ERP và file: bắt dòng commit trễ / file ghi đè có thời gian lệch. Phần lấy dư được loại ở bước dedup (ERP: Silver theo khoá; file: hash nội dung ở Bronze).
- Settle (5 phút): bỏ qua file vừa sửa, có thể đang ghi dở.
- Cửa sổ thực tế của mỗi lượt ghi vào `meta.ingestion_batch`.

### Ghi nhận và khôi phục lỗi (quy tắc commit)
Mỗi entity là một đơn vị commit độc lập; không đợi toàn bộ 9 entity mới ghi watermark.

1. Copy tạo landing bất biến. DB có một batch cho mỗi entity/window; file có một batch cho mỗi file. Tạo audit `PREPARED` cho toàn bộ input của entity trước khi sửa Bronze.
2. `nb_brz_load` đọc từng input và thay phần Bronze của đúng `batch_id` bằng một thao tác Delta nguyên tử; sau đó chuyển audit tương ứng sang `BRONZE_WRITTEN`.
3. Chỉ khi mọi input của entity ở trạng thái cuối (COMMITTED/SKIPPED_DUPLICATE/NO_NEW_DATA) và không còn PREPARED/BRONZE_WRITTEN, MERGE watermark cho entity (không lùi), rồi chuyển các dòng BRONZE_WRITTEN sang COMMITTED. Duplicate đã xác nhận cũng được tính là đã xử lý.
4. Watermark entity không tiến giữa chừng khi còn file chưa xử lý. Nếu process bị ngắt, retry cùng batch hoàn tất các input PREPARED/BRONZE_WRITTEN.
5. Khi retry, `PREPARED` thì ghi lại cùng batch atomically; `BRONZE_WRITTEN` thì không ghi Bronze lần nữa, chỉ hoàn tất watermark và audit. `COMMITTED` thì bỏ qua hoặc chạy rerun có chủ đích.
6. Nếu entity A commit xong nhưng entity B lỗi, A giữ checkpoint; B retry độc lập. Pipeline tổng thể vẫn FAILED trên Monitoring hub.

`DELETE WHERE _batch_id = X` rồi append không được coi là atomic: lỗi giữa hai lệnh có thể làm mất batch. Ưu tiên Delta overwrite giới hạn bằng `replaceWhere` trên `_batch_id`; xác minh trên Fabric Runtime rằng thao tác tạo một commit nguyên tử. Nếu runtime không hỗ trợ đúng cách, chốt một phương án MERGE khác trước khi viết notebook.

### Batch identity khi retry
`batch_id` đại diện cho cùng một tập input, không đơn thuần cho lần chạy pipeline. Chốt quy tắc: recovery và rerun cùng input phải dùng lại landing + `batch_id`. `p_batch_id` là tùy chọn để replay riêng một batch; nếu bỏ trống, `rerun` lấy các batch của `p_load_date` còn cần recovery hoặc đã COMMITTED. Một lượt normal mới có cửa sổ/landing mới là batch mới và có thể tạo raw overlap do lookback; Silver dedup theo khóa + quy tắc phiên bản. `reprocess` chủ ý lấy lại nguồn thành batch mới, ghi rõ range.

## 4. Ba kiểu chạy

| Kiểu (`p_mode`) | Khi nào | Lấy dữ liệu | Kết quả |
|---|---|---|---|
| **normal** | Lịch hằng ngày; sau lỗi chỉ khi không còn batch cần recovery | Copy từ nguồn, cửa sổ từ `watermark_state` | Tạo batch mới; overlap/lookback được giữ trong raw và xử lý ở Silver |
| **rerun** (cùng input) | Phát lại một batch theo `p_batch_id`, hoặc tất cả batch phù hợp trong `p_load_date`; gồm phục hồi PREPARED/BRONZE_WRITTEN hoặc kiểm tra code với batch COMMITTED | **Không Copy.** Đọc đúng `landing_path` đã đăng ký | Batch cũ được hoàn tất/ghi đè nguyên tử; watermark không lùi |
| **reprocess** | Nguồn đã sửa dữ liệu cũ; cần lấy lại một khoảng | Copy với `p_window_start` chỉ định (Bước 7) | Có thể khác — đúng, vì input khác |

- S2 ("chạy lại ra cùng kết quả") chỉ áp dụng cho **rerun** (cùng input = landing bất biến + cùng code).
- Rerun dùng landing vì nguồn có thể không còn giữ dữ liệu cũ (ERP master đã ghi đè, file có thể bị xoá).

## 5. File: gửi lại, trùng, đến muộn

Hợp đồng nguồn (design/01): file gửi lại = **ghi đè cùng tên**; storage gán LastModified = thời điểm ghi; nguồn **không** giữ thời gian sửa cũ.

| Tình huống | Ingest nhìn thấy? | Xử lý |
|---|---|---|
| File mới | có (LastModified trong cửa sổ) | nạp |
| Ghi đè cùng tên, nội dung đổi | có (LastModified mới) | hash khác → nạp như **phiên bản mới**; Silver lấy bản `updated_at` mới nhất theo khoá |
| Ghi đè y hệt / bị chép lại lần nữa do lookback | có | hash trùng file đã COMMITTED trong cùng source/entity → `SKIPPED_DUPLICATE`; recovery cùng `batch_id` được xử lý trước dedup toàn cục |
| Bản copy khác tên | có | hash trùng → `SKIPPED_DUPLICATE` |
| File đến với LastModified cũ (vd nguồn giữ thời gian gốc khi upload) | chỉ khi còn trong lookback | ngoài lookback → **không thấy**. Chỉ phát hiện chắc chắn nếu có expected-file inventory/release log; freshness chỉ phát hiện thiếu batch, không chứng minh nguyên nhân. Đây là giới hạn theo hợp đồng nguồn hiện tại |

Hash = `sha2(nội dung file)` tính trong `nb_brz_load` (đọc landing bằng `binaryFile`). Phạm vi dedup bắt buộc là `(source_system, entity, content_hash)`; cùng bytes ở hai entity khác nhau không được triệt tiêu lẫn nhau. Quyết định hiện phù hợp với file nhỏ; đo trước khi áp dụng cho file lớn.

## 6. Xoá ở nguồn
- Watermark không phát hiện hard delete.
- Master: so snapshot liên tiếp ở Silver → khoá biến mất = xoá (`is_deleted`).
- Orders: hợp đồng nói không xoá. Nếu nguồn có **CDC**, CDC là lựa chọn ưu tiên để nhận cả update lẫn delete.

## 7. Pipeline `pl_ingest`

```
Parameters  p_load_date, p_mode (normal | rerun | reprocess; mặc định normal), p_batch_id (rerun/recovery), p_window_start (reprocess, Bước 7)
set_run_start         v_run_start = utcNow('yyyy-MM-dd HH:mm:ss')
lkp_source_entity     T-SQL: cfg_source_entity ⋈ watermark_state → mỗi entity:
                      load_strategy, source_object, watermark_column, watermark_before, window_start, window_end
fe_source_entity      items = rerun ? [] : output Lookup
  sw_source_type      on = startsWith(load_strategy, 'db_') ? 'db' : 'file'
    db      → cp_db_to_landing     full: SELECT * ; incremental: WHERE theo cửa sổ
    file    → cp_file_to_landing   Binary, lọc LastModified [window_start, window_end)
    default → fail_unknown_source_type
nb_brz_load(p_load_date, p_run_id, p_mode, p_plan = output Lookup JSON)
                      → Bronze + ingestion_batch + watermark_state; rerun/recovery nhận p_batch_id
```
Trạng thái lượt chạy, thời lượng, lỗi từng activity: **Monitoring hub** (design/04 §1). Bất kỳ activity nào lỗi → pipeline FAILED trên Fabric; không cần notebook ghi lại.

Landing: `Files/landing/<source>/<entity>/load_date=<d>/batch=<RunId>/` — thư mục mới mỗi lượt, không ghi đè.

## 8. Số liệu Copy dùng cho đối soát

| Nhánh | Copy output có | Dùng |
|---|---|---|
| db (table → parquet) | `rowsRead`, `rowsCopied` | so với số dòng Bronze của batch |
| file (Binary) | `filesRead`, `filesWritten`, `dataRead` — **không có số dòng** | so số file Copy ghi với số file trong `ingestion_batch`; số dòng chỉ biết sau parse (`bronze_row_count`) |

Bước 1: xem các số này trong Monitoring hub khi kiểm thử. Thiết kế hiện chưa chuyển được metric từng Copy activity vào notebook/`ingestion_batch`; do đó Source↔Bronze chỉ là kiểm thử thủ công ở Bước 1. Nếu muốn recon tự động ở Bước 6 thì K7 là dependency bắt buộc, không phải spike tùy chọn.

## Quyết định
- ADR 012: Copy cho mọi nguồn; 3 chiến lược; cửa sổ ERP theo **đồng hồ nguồn** (virtual clock), file theo giờ thật + lookback + hash.

## Còn mở (spike)
- **K5:** metadata file **nguồn** (size, LastModified) cho `ingestion_batch` — Copy session log / Get Metadata. Không chặn: dedup đã dựa vào hash.
- **K6:** `%%configure -f` khi notebook chạy từ pipeline.
- **K7 (trước recon tự động Bước 6):** chứng minh output Copy (`rowsRead`, `filesWritten`) của từng entity được truyền tới bước recon và lưu bền vững; nếu không đạt, giới hạn Source↔Bronze ở kiểm tra thủ công và không mô tả nó là recon tự động.

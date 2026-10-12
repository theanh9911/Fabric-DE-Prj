# 05 — Bronze

> Bronze = **bằng chứng về nguồn**: đúng những gì nguồn gửi, dạng bảng, kèm metadata để truy vết và replay.

## 1. Quy tắc

| Quy tắc | Chi tiết |
|---|---|
| Append theo batch | Mỗi batch mới được thêm; chỉ thay đúng batch đó bằng một commit Delta nguyên tử khi recovery/rerun |
| Không làm sạch | Không trim, không parse, không dedup, không lọc dòng lỗi |
| Giữ raw **theo khả năng của nguồn** | **ERP:** giữ kiểu gốc từ parquet (timestamp, decimal) — nguồn đã có kiểu, ép về chữ là mất thông tin vô ích. **CSV:** mọi cột string (file vốn là chữ; parse có chủ đích ở Silver) |
| Mọi snapshot master được giữ | Nguồn lịch sử duy nhất cho SCD2 và phát hiện xoá |
| Landing vẫn giữ file gốc | Bronze hỏng → dựng lại từ landing |

## 2. Bảng và cột

`brz.brz_<source>_<entity>` — 9 bảng. Cột nghiệp vụ như nguồn + cột kỹ thuật:

| Cột | Giá trị |
|---|---|
| `_source_system`, `_entity` | từ config |
| `_source_file` | đường dẫn file landing (file) / landing parquet (db) |
| `_load_date` | `p_load_date` |
| `_batch_id` | từ `ingestion_batch` |
| `_run_id` | RunId |
| `_ingested_at` | UTC lúc ghi |
| `_snapshot_at` | (full snapshot) thời điểm chụp = `window_end` |

Partition: không partition lúc đầu (dữ liệu nhỏ). Xem lại ở bài đo hiệu năng (09 §5).

## 3. Idempotent
- Ghi theo batch bằng một thao tác Delta nguyên tử/idempotent. Không dùng `DELETE` rồi append như hai lệnh độc lập vì lỗi xen giữa có thể để Bronze mất batch. Cơ chế cụ thể phải được xác minh trên Fabric Runtime trước khi chốt notebook.
- **Rerun/recovery** chọn đúng `p_batch_id`, đọc lại landing bất biến và **giữ nguyên `batch_id`**. `PREPARED` có thể thay phần batch trong Bronze bằng thao tác nguyên tử; `BRONZE_WRITTEN` chỉ hoàn tất watermark/audit. Với batch COMMITTED, rerun code phải cho kết quả y hệt. Mỗi attempt mới có `ingestion_batch_id` riêng và trỏ cùng `batch_id`.
- File có `content_hash` trùng một file đã COMMITTED trong cùng `(source_system, entity)` (gửi lại y hệt, bản copy khác tên, file lấy lại do lookback) → **không ghi Bronze**, `ingestion_batch.status = SKIPPED_DUPLICATE`. Recovery/rerun theo đúng `batch_id` xử lý trước bước dedup toàn cục, để một file không tự loại chính nó.
- Commit theo từng entity: ghi audit PREPARED → Bronze commit nguyên tử → audit BRONZE_WRITTEN → cập nhật watermark entity bằng MERGE không lùi → audit COMMITTED. Lỗi ở entity sau không rollback các entity đã commit; retry tiếp tục từ stage của entity đó. Nếu crash sau Bronze commit nhưng trước audit/checkpoint, batch cùng ID được ghi thay atomically rồi hoàn tất metadata, không append thêm.

## 4. Schema drift
| Tình huống | Bronze | Silver/Gold |
|---|---|---|
| Cột mới | Ghi nhận (`mergeSchema`) + `ingestion_batch.schema_changes` + cảnh báo | **Không tự động** lan xuống; mở rộng có chủ đích (sửa SQL, review) |
| Cột mất | Giá trị NULL cho batch mới + cảnh báo | Silver DQ bắt |
| Đổi kiểu (ERP) | Batch **fail** (không tự ép) + cảnh báo | — |

## Quyết định
- ERP giữ kiểu; CSV string. Drift: Bronze ghi nhận, downstream mở rộng có chủ đích.

## Còn mở
- Cách đọc CSV an toàn với dấu `"`, xuống dòng trong ô — kiểm trên file thật ở Bước 1.

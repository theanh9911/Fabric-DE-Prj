# 05 — Bronze

> Bronze = **bằng chứng về nguồn**: đúng những gì nguồn gửi, dạng bảng, kèm metadata để truy vết và replay.

## 1. Quy tắc

| Quy tắc | Chi tiết |
|---|---|
| Append-only | Mỗi batch thêm vào; không sửa dữ liệu đã có (trừ thay batch khi rerun) |
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
- Ghi theo batch: `DELETE WHERE _batch_id = X` rồi append.
- **Rerun** đọc lại đúng thư mục landing của batch đã COMMITTED và **giữ nguyên `batch_id`** → xoá rồi ghi lại đúng phần đó → Bronze y hệt. `ingestion_batch` thêm 1 dòng `run_mode = rerun` trỏ cùng `batch_id`.
- Bước ghi Bronze + `ingestion_batch` COMMITTED + `watermark_state` theo thứ tự đó; lỗi giữa chừng → watermark chưa tiến → lần sau lấy lại.

## 4. Schema drift
| Tình huống | Bronze | Silver/Gold |
|---|---|---|
| Cột mới | Ghi nhận (`mergeSchema`) + log vào `task_run.details_json` + cảnh báo | **Không tự động** lan xuống; mở rộng có chủ đích (sửa SQL, review) |
| Cột mất | Giá trị NULL cho batch mới + cảnh báo | Silver DQ bắt |
| Đổi kiểu (ERP) | Batch **fail** (không tự ép) + cảnh báo | — |

## Quyết định
- ERP giữ kiểu; CSV string. Drift: Bronze ghi nhận, downstream mở rộng có chủ đích.

## Còn mở
- Cách đọc CSV an toàn với dấu `"`, xuống dòng trong ô — kiểm trên file thật ở Bước 1.

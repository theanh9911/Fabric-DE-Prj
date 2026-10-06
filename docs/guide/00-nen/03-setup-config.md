# 0.3 — `nb_setup_config`: ghi config ✅

> Nội dung config được thay ở [Bước 1.3](../01-ingest/01-meta-setup.md). Cách làm (MERGE từ VALUES) giữ nguyên.

## Mục tiêu
Bảng config trong `meta` có nội dung đúng như trong notebook.

## Hiểu trước khi làm
- **Notebook này là nguồn sự thật của config.** Không sửa tay bảng trên Fabric. Muốn đổi: sửa VALUES trong notebook → Commit → chạy lại.
- Chỉ giữ ở bảng những config mà **pipeline/runtime cần đọc** (Lookup không gọi được code Spark). Luật nghiệp vụ nằm trong code (`nb_common`) — [design/06 §2](../../design/06-silver.md).
- Mỗi bảng 1 lệnh:
  ```sql
  MERGE INTO meta.<bảng> t
  USING (SELECT * FROM VALUES (...), (...) AS v(cột1, cột2, ...)) s   -- dòng viết tay
  ON t.<khoá> = s.<khoá>
  WHEN MATCHED THEN UPDATE SET *            -- có rồi → cập nhật theo script
  WHEN NOT MATCHED THEN INSERT *            -- chưa có → thêm
  WHEN NOT MATCHED BY SOURCE THEN DELETE;   -- bảng có mà script không có → xoá
  ```
  → bảng luôn khớp đúng notebook; chạy lại bao nhiêu lần cũng được; lịch sử thay đổi nằm trong Git.

## Lịch sử
| Phiên bản | Nội dung |
|---|---|
| v2 (Bước 0) | `cfg_source_entity` 9 dòng (`source_type`, `load_type`…) · `ref_order_status` 4 · `ref_value_mapping` 4 |
| v4 (Bước 1.3) | Chỉ `cfg_source_entity` 9 dòng với `load_strategy` (3 chiến lược) |

## Làm
Mở `nb_setup_config` → **Run all** → Stop session.

## Kết quả mong đợi
Cell cuối in số dòng từng bảng config (v4: `cfg_source_entity = 9`). Lần đầu MERGE báo `num_inserted_rows`; chạy lại báo `num_updated_rows` — bình thường.
